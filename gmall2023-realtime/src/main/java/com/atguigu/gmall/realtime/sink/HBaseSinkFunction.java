package com.atguigu.gmall.realtime.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {


    private Connection conn;

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConn(conn);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = HBaseUtil.getHBaseConnection();
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> tp, Context ctx) throws Exception {
        // insert update delete
        JSONObject data = tp.f0;
        String opType = data.getString("op_type");

        if ("delete".equals(opType)) {
            // 删除维度信息
            delDim(tp);
        } else {
            // insert 和 update 的时候，写入维度数据
            putDim(tp);
        }

    }

    private void putDim(Tuple2<JSONObject, TableProcess> t) throws IOException {
        JSONObject data = t.f0;
        TableProcess tp = t.f1;

        String rowKey = data.getString(tp.getSinkRowKey());
        // data中有多少 kv 就写多少列-1
        JSONObject dataCopy = new JSONObject();
        dataCopy.putAll(data);
        dataCopy.remove("op_type");
        HBaseUtil.putRow(conn, Constant.HBASE_NAMESPACE, tp.getSinkTable(), rowKey, tp.getSinkFamily(), dataCopy);

    }

    private void delDim(Tuple2<JSONObject, TableProcess> t) throws IOException {
        JSONObject data = t.f0;
        TableProcess tp = t.f1;

        String rowKey = data.getString(tp.getSinkRowKey());

        HBaseUtil.delRow(conn, Constant.HBASE_NAMESPACE, tp.getSinkTable(),rowKey);

    }
}









