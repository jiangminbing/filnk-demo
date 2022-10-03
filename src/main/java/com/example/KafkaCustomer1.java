package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

/**
 * @author jiangmb
 * @version 1.0.0
 * @date 2021-07-10 14:41
 */
public class KafkaCustomer1 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // 非常关键，一定要设置启动检查点！！

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-group01");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<>("test",new SimpleStringSchema(),  props);
        consumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)));
        DataStream<String> stream = env
                .addSource(consumer);
//        stream.flatMap(new MyDemoLineSplitter()).keyBy(v->v.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .apply(new MyDemoWindowFunction()).print();
        // 方法等同于
        stream.flatMap(new MyDemoLineSplitter()).keyBy(v->v.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(1).print();
        env.execute();
    }
    public static class MyDemoWindowFunction implements WindowFunction<Tuple2<String,Integer>, Tuple2<String, Integer>, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 定义相同key，在一段时间内可能收到相同key值数据集合
            int sum = 0;
            for (Tuple2<String,Integer> t: input){
                sum += t.f1;
            }
            out.collect(new Tuple2<>(key,sum));
        }
    }
    public static final class MyDemoLineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String v, Collector<Tuple2<String, Integer>> o) throws Exception {
            String str[] = v.split("\\|");
            for (int i = 0; i <str.length ; i++) {
                Tuple2<String,Integer> t = new Tuple2<>(str[i],1);
                o.collect(t);
            }
        }
    }
}
