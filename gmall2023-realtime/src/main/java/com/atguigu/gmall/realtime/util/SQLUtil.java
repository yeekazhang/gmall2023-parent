package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.Constant;

public class SQLUtil {
    public static String getKafkaDDLSource(String groupId, String topic) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'properties.group.id' = '" + groupId + "'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'json.ignore-parse-errors' = 'true'," + // 当 json 解析失败的时候,忽略这条数据
                "  'format' = 'json' " +
                ")";

    }

    public static String getKafkaDDLSink(String topic) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'format' = 'json' " +
                ")";
    }
}
