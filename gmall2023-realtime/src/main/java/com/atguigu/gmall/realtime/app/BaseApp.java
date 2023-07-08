package com.atguigu.gmall.realtime.app;

import com.atguigu.gmall.realtime.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseApp {

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);

    public void start(int port, int p, String ckAndGroupId, String topic){
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(p);

        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall2023/" + ckAndGroupId);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        KafkaSource<String> source = FlinkSourceUtil.getKafkaSource(ckAndGroupId, topic);

        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(),
                "ods_db_source");

        // 获取流之后，具体的处理和业务逻辑相关
        // do something
        handle(env, stream);


        try{
            env.execute();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
