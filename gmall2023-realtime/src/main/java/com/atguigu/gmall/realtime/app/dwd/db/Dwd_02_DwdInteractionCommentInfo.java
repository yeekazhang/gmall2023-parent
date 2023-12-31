package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Dwd_02_DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_02_DwdInteractionCommentInfo().start(
                30002,
                2,
                "Dwd_02_DwdInteractionCommentInfo"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        // 1 通过 ddl 的方法建立动态表：从 ods_db 读取数据 source
        readOdsDb(tEnv);

        // 2 过滤出评论表数据
        Table commentInfo = tEnv.sqlQuery("select " +
                " `data`['id'] id, " +
                " `data`['user_id'] user_id, " +
                " `data`['sku_id'] sku_id, " +
                " `data`['appraise'] appraise, " +
                " `data`['comment_txt'] comment_txt, " +
                " `data`['create_time'] comment_time," +
                " ts, " +
                " pt " +
                " from ods_db " +
                " where `database`='gmall2023' " +
                " and `table`='comment_info' " +
                " and `type`='insert' ");
        tEnv.createTemporaryView("comment_info", commentInfo);

        // 3 通过 ddl 方式建表：base_dic hbase 中的维度表 source
        readBaseDic(tEnv);

        // 4 事实表与维度表 join: lookup join
        Table result = tEnv.sqlQuery(
                "select " +
                        "ci.id, " +
                        "ci.user_id," +
                        "ci.sku_id," +
                        "ci.appraise," +
                        "dic.info.dic_name appraise_name," +
                        "ci.comment_txt," +
                        "ci.ts " +
                        "from comment_info ci " +
                        "join base_dic for system_time as of ci.pt as dic " +
                        "on ci.appraise=dic.dic_code");

        // 5 通过 ddl 方式建表：与 kafka 的 topic 关联 (sink)
        tEnv.executeSql("create table dwd_interaction_comment_info(" +
                " id string, " +
                " user_id string," +
                " sku_id string," +
                " appraise string," +
                " appraise_name string," +
                " comment_txt string," +
                " ts bigint" +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

        // 6 把 join 的结果写入到 sink 表
        result.executeInsert("dwd_interaction_comment_info");

    }
}
