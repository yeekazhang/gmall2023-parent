package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Dwd_03_DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_03_DwdTradeCartAdd().start(
                30003,
                2,
                "Dwd_03_DwdTradeCartAdd"
        );

    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       StreamTableEnvironment tEnv) {
        // 1 读取ods_db 数据
        readOdsDb(tEnv);

        // 2 过滤出加购数据
        Table cartAdd = tEnv.sqlQuery("select " +
                " `data`['id'] id," +
                " `data`['user_id'] user_id," +
                " `data`['sku_id'] sku_id," +
                " if(`type`='insert'," +
                "   cast(`data`['sku_num'] as int), " +
                "   cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)" +
                ") sku_num ," +
                " ts " +
                "from ods_db " +
                "where `database`='gmall2023' " +
                "and `table`='cart_info' " +
                "and (" +
                " `type`='insert' " +
                "  or(" +
                "     `type`='update' " +
                "      and `old`['sku_num'] is not null " +
                "      and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int) " +
                "   )" +
                ")");

        // 3 写出到 kafka
        tEnv.executeSql("create table dwd_trade_cart_add(" +
                "   id string," +
                "   user_id string," +
                "   sku_id string," +
                "   sku_num int," +
                "   ts bigint" +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_CART_ADD));

        cartAdd.executeInsert("dwd_trade_cart_add");

    }
}
