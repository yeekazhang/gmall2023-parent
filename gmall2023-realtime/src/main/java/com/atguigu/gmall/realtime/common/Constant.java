package com.atguigu.gmall.realtime.common;

import org.apache.flink.connector.kafka.source.KafkaSource;

public class Constant {
    public static final String KAFKA_BROKERS = "hadoop162:9092,hadoop163:9092,hadoop164:9092";

    public static final String TOPIC_ODS_DB = "ods_db";
    public static final String TOPIC_ODS_LOG = "ods_log";

    public static final String MYSQL_HOST = "hadoop162";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USERNAME = "root";
    public static final String MYSQL_PASSWORD = "aaaaaa";
    public static final String HBASE_NAMESPACE = "gmall";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop162:3306?useSSL=false";
    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
}
