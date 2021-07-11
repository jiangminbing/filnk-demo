package com.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author jiangmb
 * @version 1.0.0
 * @date 2021-07-11 17:13
 */
public class ProcessFunctionDemo {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String,String>> stream = env2.fromElements(new Tuple2<>("jiang","jiang"),new Tuple2<>("min","min"),new Tuple2<>("bing","bing"));

        // apply the process function onto a keyed stream
        DataStream<Tuple2<String, Long>> result = stream
                .keyBy(value -> value.f0)
                .process(new CountWithTimeoutFunction());
    }

    /*
     * The data type stored in the state
     */
    public static class CountWithTimestamp {

        public String key;
        public long count;
        public long lastModified;
    }

    /**
     * 维护计数和超时的 ProcessFunction 的实现
     */
    public static class CountWithTimeoutFunction
            extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, Long>> {

        /** 此流程功能维护的状态 */
        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
        }

        @Override
        public void processElement(
                Tuple2<String, String> value,
                Context ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {

            // 检索当前计数
            CountWithTimestamp current = state.value();
            if (current == null) {
                current = new CountWithTimestamp();
                current.key = value.f0;
            }

            // update the state's count
            current.count++;

            // set the state's timestamp to the record's assigned event time timestamp
            current.lastModified = ctx.timestamp();

            // write the state back
            state.update(current);

            // 从当前事件时间开始安排下一个计时器 60 秒
            ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {

            // get the state for the key that scheduled the timer
            CountWithTimestamp result = state.value();

            // check if this is an outdated timer or the latest timer
            if (timestamp == result.lastModified + 60000) {
                // emit the state on timeout
                out.collect(new Tuple2<String, Long>(result.key, result.count));
            }
        }
    }
}
