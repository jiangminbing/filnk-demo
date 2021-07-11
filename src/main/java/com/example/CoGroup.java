package com.example;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
/**
 * @author jiangmb
 * @version 1.0.0
 * @date 2021-07-10 16:21
 */
public class CoGroup {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //watermark 自动添加水印调度时间
        //env.getConfig().setAutoWatermarkInterval(200);

        List<Tuple3<String, String, Integer>> tuple3List1 = Arrays.asList(
                new Tuple3<>("伍七", "girl", 18),
                new Tuple3<>("吴八", "man", 30)
        );
        List<Tuple3<String, String, Integer>> tuple3List2 = Arrays.asList(
                new Tuple3<>("伍七", "girl", 18),
                new Tuple3<>("吴八", "man", 30)
        );
        //Datastream 1
        DataStream<Tuple3<String, String, Integer>> dataStream1 = env.fromCollection(tuple3List1)
                //添加水印窗口,如果不添加，则时间窗口会一直等待水印事件时间，不会执行apply
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, timestamp) -> System.currentTimeMillis()));
        //Datastream 2
        DataStream<Tuple3<String, String, Integer>> dataStream2 = env.fromCollection(tuple3List2)
                //添加水印窗口,如果不添加，则时间窗口会一直等待水印事件时间，不会执行apply
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Integer>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Integer> element, long timestamp) {
                                return System.currentTimeMillis();
                            }
                        })
                );

        //对dataStream1和dataStream2两个数据流进行关联，没有关联也保留
        //Datastream 3
//        DataStream<String> newDataStream = dataStream1.coGroup(dataStream2)
//                .where(new KeySelector<Tuple3<String, String, Integer>, String>() {
//                    @Override
//                    public String getKey(Tuple3<String, String, Integer> value) throws Exception {
//                        return value.f1;
//                    }
//                })
//                .equalTo(t3->t3.f1)
//                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
//                .apply(new CoGroupFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>, String>() {
//                    @Override
//                    public void coGroup(Iterable<Tuple3<String, String, Integer>> first, Iterable<Tuple3<String, String, Integer>> second, Collector<String> out) throws Exception {
//                        StringBuilder sb = new StringBuilder();
//                        //datastream1的数据流集合
//                        for (Tuple3<String, String, Integer> tuple3 : first) {
//                            sb.append(JSON.toJSONString(tuple3)).append("\n");
//                        }
//                        //datastream2的数据流集合
//                        for (Tuple3<String, String, Integer> tuple3 : second) {
//                            sb.append(JSON.toJSONString(tuple3)).append("\n");
//                        }
//                        out.collect(sb.toString());
//                    }
//                });
//        newDataStream.print();
        dataStream1.join(dataStream2)
                .where(v->v.f0).equalTo(v->v.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new JoinFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> join(Tuple3<String, String, Integer> t1, Tuple3<String, String, Integer> t2) throws Exception {
                        return new Tuple3<>(t1.f0+"-"+t2.f0,t1.f1+"-"+t2.f1,t1.f2+"-"+t1.f2);
                    }
                }).print();
        env.execute("flink CoGroup job");
    }
}
