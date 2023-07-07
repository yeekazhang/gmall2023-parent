package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.sink.HBaseSinkFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class FlinkSinkUtil {

    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getHBaseSink() {
        return new HBaseSinkFunction();
    }
}
