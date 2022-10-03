package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Arrays;
import java.util.Random;

/**
 * @author jiangmb
 * @version 1.0.0
 * @date 2021-07-11 22:40
 */
public class Watermarks {
//    public static void main(String[] args) throws Exception {
//        // 设置水印
//        /**
//         * 水映的理解
//         * https://blog.csdn.net/shudaqi2010/article/details/114868234?utm_term=flink%E4%B9%B1%E5%BA%8F%E6%B0%B4%E5%8D%B0&utm_medium=distribute.pc_aggpage_search_result.none-task-blog-2~all~sobaiduweb~default-5-114868234&spm=3001.4430
//         */
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //  数据的事件时间
//        DataStream<Tuple2<String, Long>> dataStream = env.fromCollection(Arrays.asList(new Tuple2("jiang",1626070486903L)));
//        dataStream.assignTimestampsAndWatermarks(
//                WatermarkStrategy
//                        .<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                        .withIdleness(Duration.ofSeconds(5))
//                        .withTimestampAssigner((event, timestamp)->event.f1));
//        env.execute();
//    }
    public static class MyLog {
        private String msg;
        private Integer cnt;
        private long timestamp;

    public MyLog(String msg, Integer cnt, long timestamp) {
        this.msg = msg;
        this.cnt = cnt;
        this.timestamp = timestamp;
    }

    public MyLog() {
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Integer getCnt() {
        return cnt;
    }

    public void setCnt(Integer cnt) {
        this.cnt = cnt;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}

    public static class MySourceFunction implements SourceFunction<MyLog> {

        private boolean running = true;

        @Override
        public void run(SourceContext<MyLog> ctx) throws Exception {
            while (true) {
                Thread.sleep(1000);
                String[] str = {"A","B","C","D"};
                int i = (int)(Math.random()*10);
                ctx.collect(new MyLog(str[i],1,System.currentTimeMillis()));
            }
        }
        @Override
        public void cancel() {
            this.running = false;
        }
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 数据源使用自定义数据源，每1s发送一条随机消息
        env.addSource(new MySourceFunction())
                // 指定水印生成策略是，最大事件时间减去 5s，指定事件时间字段为 timestamp
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                <MyLog>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event,timestamp)->event.timestamp))
                // 按 消息分组
                .keyBy((event)->event.msg)
                // 定义一个10s的时间窗口
                .timeWindow(Time.seconds(10))
                // 统计消息出现的次数
                .sum("cnt")
                // 打印输出
                .print();

        env.execute("log_window_cnt");
    }
}
