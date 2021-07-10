package com.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author jiangmb
 * @version 1.0.0
 * @date 2021-07-08 22:32
 */
public class DateStreamApi {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<Tuple2<String, Integer>> dataStream = env
//                .socketTextStream("localhost", 9999)
//                .flatMap(new Splitter())
//                .keyBy(value -> value.f0)
//                // 执行时间窗口 5秒钟之内统计出现次数最多的车
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .sum(1);
//
//        dataStream.print();
//
//        env.execute("Window WordCount");
//        //  数据源 默认file 1.读取文本 2.hdfs 标明读取hdfs文件
//        DataStream<String> d1 = env.readTextFile("hdfs://test/data");
//        DataStream<String> d2 = env.readTextFile("D:\\test.txt");
//        /**
//         * readFile(fileInputFormat, path, watchType, interval, pathFilter, typeInfo)-
//         * 这是前两个内部调用的方法。
//         * 它path根据给定的fileInputFormat.
//         * 根据提供的watchType，
//         * 此源可能会定期（每interval毫秒）
//         * 监视新数据的路径 ( FileProcessingMode.PROCESS_CONTINUOUSLY)，
//         * 或处理当前路径中的数据并退出 ( FileProcessingMode.PROCESS_ONCE)。使用pathFilter，用户可以进一步排除正在处理的文件
//         */
//        DataStream<String> d3 =  env.readFile(new TextInputFormat(new Path("D:\\data\\test.txt")),
//                "D:\\test.txt", FileProcessingMode.PROCESS_CONTINUOUSLY, 10, FilePathFilter.createDefaultFilter().filterPath("D:\\data\\test"))
//         // socket 监听
//        env.socketTextStream("localhost",9000);
//        // 添加数据源
//        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", "localhost:9092");
//        props.setProperty("group.id", "flink-group");
//        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.setProperty("auto.offset.reset", "latest");
//        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<>("test",new SimpleStringSchema(),  props);
//        env.addSource(consumer);
//        // 数据集合
//        DataStream<Integer> d4 =  env.fromCollection(Arrays.asList(1,2,3));
//        DataStream<Integer> d5 = env.fromCollection(Arrays.asList(1,24,5).iterator(),Integer.class);
//
//        // 数据输出
//        d5.writeAsText("d://test.txt");
//        d5.writeAsCsv("d://test.csv");
//        // 添加一个通道
//        d5.addSink(new SinkFunction<Integer>() {
//            @Override
//            public void invoke(Integer value, Context context) throws Exception {
//                SinkFunction.super.invoke(value, context);
//            }
//        });
        // 迭代器
//        DataStream<Long> someIntegers = env.generateSequence(0, 1000);
//
//        IterativeStream<Long> iteration = someIntegers.iterate();
//
//        DataStream<Long> minusOne = iteration.map((MapFunction<Long, Long>) value -> value - 1);
//
//        DataStream<Long> stillGreaterThanZero = minusOne.filter((FilterFunction<Long>) value -> (value > 0));
//
//        iteration.closeWith(stillGreaterThanZero);
//
//        DataStream<Long> lessThanZero = minusOne.filter((FilterFunction<Long>) value -> (value <= 0));
//
//        lessThanZero.print();
//
//        env.execute();
        // 创建本地程序
        final StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStream<String> lines = env.fromElements("1","2","555");

//        // 数据转换 算子方法
//        DataStream<Integer> d7 = lines.map(d->Integer.valueOf(d));
//
//        // 将结果切割转换
//        lines.flatMap((FlatMapFunction<String, String>) (value, out) -> {
//            for(String word: value.split(" ")){
//                out.collect(word);
//            }
//        });
//        // 过滤器
//        DataStream d8 = lines.filter(d->d.length() < 7);

        // 将相同键分到相同区
        DataStream<Person> personDateStream = env2.fromElements(new Person("zhang",1),
                new Person("jiang",5),new Person("zhang",1));
        personDateStream.keyBy(p->p.getName()).sum("age").print();
        env2.execute();



    }
    public static class Person {
        private String name;

        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public Person() {
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }


}
