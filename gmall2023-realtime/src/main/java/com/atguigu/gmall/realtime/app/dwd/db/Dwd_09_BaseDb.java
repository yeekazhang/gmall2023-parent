package com.atguigu.gmall.realtime.app.dwd.db;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseApp;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.util.HBaseUtil;
import com.atguigu.gmall.realtime.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@Slf4j
public class Dwd_09_BaseDb extends BaseApp {
    public static void main(String[] args) {
        new Dwd_09_BaseDb().start(20001, 2, "DimApp", Constant.TOPIC_ODS_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        // 1 对消费的数据做数据清洗
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);

        // 2 通过 flink cdc 读取配置表的数据
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(env);

        // 4 数据流去 connect 配置流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dimDataWithTpStream = connect(etledStream, tpStream);

        // 5 删除不需要的列
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream = deleteNotNeedColumns(dimDataWithTpStream);

        // 6 写出到 Kafka 中
        writeToKafka(resultStream);


    }

    private void writeToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream) {
        resultStream.sinkTo(FlinkSinkUtil.getKafkaSink());
    }


    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> deleteNotNeedColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dimDataWithTpStream) {
        return dimDataWithTpStream
                .map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> tp) throws Exception {

                        JSONObject data = tp.f0;
                        List<String> columns = new ArrayList<>(Arrays.asList(tp.f1.getSinkColumns().split(",")));
                        columns.add("op_type");
                        data.keySet().removeIf(key -> !columns.contains(key));

                        return tp;
                    }
                });
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(SingleOutputStreamOperator<JSONObject> dataStream,
                                                                                 SingleOutputStreamOperator<TableProcess> tpStream) {
        // 1 把配置流做成广播流
        // key 表名：type  user_info:ALL
        // value: TableProcess
        MapStateDescriptor<String, TableProcess> desc = new MapStateDescriptor<String, TableProcess>("tp", String.class, TableProcess.class);
        BroadcastStream<TableProcess> tpBcStream = tpStream.broadcast(desc);

        // 2 数据流去 connect 广播流
        return dataStream
                .connect(tpBcStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {

                    private HashMap<String, TableProcess> map;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // open 中没有办法访问状态
                        map = new HashMap<>();

                        // 1 去 mysql 中查询 table_process 表所有数据
                        java.sql.Connection mysqlConn = JdbcUtil.getMysqlConnection();
                        List<TableProcess> tps = JdbcUtil.queryList(mysqlConn, "select * from gmall2023_config.table_process where sink_type=?", new Object[]{"dwd"}, TableProcess.class, true);
                        for (TableProcess tp : tps) {
                            String key = getKey(tp.getSourceTable(), tp.getSourceType());
                            map.put(key, tp);
                        }
                        JdbcUtil.closeConnection(mysqlConn);
                    }

                    // 4 处理数据流中的数据：从广播状态中读取配置信息
                    @Override
                    public void processElement(JSONObject obj,
                                               ReadOnlyContext readOnlyContext,
                                               Collector<Tuple2<JSONObject, TableProcess>> collector) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcess> state = readOnlyContext.getBroadcastState(desc);
                        String key = getKey(obj.getString("table"), obj.getString("type"));
                        TableProcess tp = state.get(key);

                        if (tp == null) {
                            tp = map.get(key);
                        }

                        if(tp != null){
                            JSONObject data = obj.getJSONObject("data");
                            data.put("op_type", obj.getString("type"));  // 后期需要
                            collector.collect(Tuple2.of(data, tp));
                        }

                    }

                    // 3 处理广播流中的数据：把配置信息存入到广播状态中
                    @Override
                    public void processBroadcastElement(TableProcess tp,
                                                        Context context,
                                                        Collector<Tuple2<JSONObject, TableProcess>> collector) throws Exception {
                        BroadcastState<String, TableProcess> state = context.getBroadcastState(desc);
                        String key = getKey(tp.getSourceTable(), tp.getSourceType());

                        if("d".equals(tp.getOp())){
                            // 删除状态
                            state.remove(key);
                            // map中的配置也要删除
                            map.remove(key);
                        } else {
                            state.put(key, tp);
                        }

                    }

                    private String getKey(String table, String type) {
                        return table + ":" + type;
                    }
                });


    }

    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("gmall2023_config")
                .tableList("gmall2023_config.table_process")
                .username(Constant.MYSQL_USERNAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        return env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc-source")
                .setParallelism(1)  // 并行度设置为1
                .map(new MapFunction<String, TableProcess>() {
                    @Override
                    public TableProcess map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value);
                        String op = obj.getString("op");
                        TableProcess tp;

                        if ("d".equals(op)){
                            tp = obj.getObject("before", TableProcess.class);
                        } else {
                            tp = obj.getObject("after", TableProcess.class);
                        }

                        tp.setOp(op);

                        return tp;
                    }
                })
                .filter(op -> "dwd".equals(op.getSinkType())); // 过滤出事实表的配置信息


    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                try {
                    JSONObject obj = JSON.parseObject(value);
                    String db = obj.getString("database");
                    String type = obj.getString("type");
                    String data = obj.getString("data");

                    return "gmall2023".equals(db)
                            && ("insert".equals(type)
                            || "update".equals(type))
                            && data != null
                            && data.length() > 2;

                } catch (Exception e) {
                    log.warn("不是正确的 json 格式的数据:" + value);
                    return false;
                }
            }
        })
        .map((MapFunction<String, JSONObject>) JSON::parseObject);
    }
}











