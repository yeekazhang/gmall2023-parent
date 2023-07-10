package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Dwd_06_DwdTradeOrderPaymentSuccess extends BaseSQLApp {

    public static void main(String[] args) {
        new Dwd_06_DwdTradeOrderPaymentSuccess().start(
                30006,
                2,
                "Dwd_06_DwdTradeOrderPaymentSuccess"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        // 1 读取下单事务事实表
        tEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint," +
                        "et as to_timestamp_ltz(ts, 0), " +
                        "watermark for et as et - interval '3' second " +
                        ")" + SQLUtil.getKafkaDDLSource("dwd_trade_order_detail", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL)
        );

        // 2 读取 ods_db
        readOdsDb(tEnv);

        // 3 读取字典表
        readBaseDic(tEnv);

        // 4 从 ods_db 中过滤 payment_info
        Table paymentInfo = tEnv.sqlQuery(
                "select " +
                        "data['user_id'] user_id," +
                        "data['order_id'] order_id," +
                        "data['payment_type'] payment_type," +
                        "data['callback_time'] callback_time," +
                        "`pt`," +
                        "`ts`, " +
                        "`et` " +
                        "from ods_db " +
                        "where `database`='gmall2023' " +
                        "and `table`='payment_info' " +
                        "and `type`='update' " +
                        "and `old`['payment_status'] is not null " +
                        "and `data`['payment_status']='1602' "
        );
        tEnv.createTemporaryView("payment_info", paymentInfo);

        // 5 3张 join：interval join 无需设置 ttl
        Table result = tEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code ," +
                        "dic.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id=od.order_id " +
                        "and od.et >= pi.et - interval '30' minute " +
                        "and od.et <= pi.et + interval '5' second " +
                        "join base_dic for system_time as of pi.pt as dic " +
                        "on pi.payment_type=dic.dic_code "
        );

        // 6 写出到kafka中
        tEnv.executeSql(
                "create table dwd_trade_order_payment_success(" +
                        "order_detail_id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "payment_type_code string," +
                        "payment_type_name string," +
                        "callback_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_payment_amount string," +
                        "ts bigint " +
                        ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS)
        );
        result.executeInsert("dwd_trade_order_payment_success");

    }
}












