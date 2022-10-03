package com.example;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author jiangmb
 * @version 1.0.0
 * @date 2021-07-11 15:59
 */
public class WindowDemo {
    public static void main(String[] args) throws Exception {
        // 创建本地程序
        final StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();

        // 将相同键分到相同区
        DataStream<Person> personDateStream = env2.fromElements(new Person("zhang",1),
                new Person("jiang",5),new Person("zhang",1),new Person("zhang",1));
        personDateStream.keyBy(p->p.getName()).sum("age").print();

        /**
         * 运算
         * 结果
         * 5> Person{name='zhang', age=1}
         * 15> Person{name='jiang', age=5}
         * 5> Person{name='zhang_zhang', age=2}
         * 5> Person{name='zhang_zhang_zhang', age=3}
         * 流中的数据是一个个流入，随着流入的数据进行累计，当然可以设置时间窗口，设置时间端进行累计计算出结果
         */
        personDateStream.keyBy(p-> p.getName()).reduce((a,b)->{
            Person p = new Person();
            p.setAge(a.age+b.age);
            p.setName(a.getName()+"_"+b.getName());
            return p;
        }).print();
//         对5s内的流入的数据进行分组
        /**
         * 窗口定义
         * 1.滚动窗口（Tumbling Windows）：滚动窗口是根据固定的时间或大小进行切割，且窗口和窗口之间的元素互不重叠。DataStream API 基于Event Time 和 Process Time 两种时间类型的Tumbling 窗口，对应的Assigner分别为TumblingEventTime和TumblingProcessTime。
         * 2.划动窗口（Sliding Windows）: 滑动窗口是在滚动窗口基础之上增加来窗口的滑动时间（Slide Time），且允许窗口数据发生重叠。DataStream API 基于Event Time 和 Process Time 两种时间类型的Tumbling 窗口，对应的Assigner分别为SlidingEventTime和SlidingProcessTime。
         * 3.会话窗口（Ssession Windows）：会话窗口是将某段时间内活跃度较高的数据聚合成一个窗口进行计算，窗口的触发条件是Session Gap，在规定的时间内如果没有数据活跃接入，则认为窗口结束，触发窗口计算结果。
         * 4.全局窗口(Global Windows):全局窗口就是将所有相同的key的数据分配到单个窗口中进行计算，窗口没有起始和结束时间，窗口需要借助Trigger来触发计算，如果对Global Windows 指定Trigger ，窗口是不会触发计算
         */
        // 滚动窗口
        WindowedStream<Person, String, TimeWindow> windowedStream = personDateStream.keyBy(p->p.getName())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        personDateStream.keyBy(p->p.getName()).window(TumblingEventTimeWindows.of(Time.seconds(5)));
        // 时间窗口10s 划动时间是10s
        personDateStream.keyBy(p->p.name).window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(10)));
        // 会话窗口
        personDateStream.keyBy(p->p.name).window(EventTimeSessionWindows.withGap(Time.minutes(1)));
        // 全局窗口
        personDateStream.keyBy(p->p.name).window(GlobalWindows.create());

        /**
         * windowAll
         */
        personDateStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        /**
         * 对窗口函数进行操作
         */
        windowedStream.apply(new MyWindowFunction()).print();
        /**
         * window 窗口进行reduce
         */
        DataStream<Tuple2<String,Integer>> dataStream = env2.fromCollection(Arrays.asList(new Tuple2("jiang",1),new Tuple2<>("min",2),new Tuple2<>("bing",3)));
        dataStream.keyBy(v->v.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).
                reduce((ReduceFunction<Tuple2<String, Integer>>) (stringIntegerTuple2, t1) -> null).
                returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class));
        // 两种方式进行声明 第一中需要声明 返回泛型的类型
        dataStream.keyBy(v->v.f0).reduce(new MyReduceFunction());

        DataStream<Tuple2<String, Long>> input = env2.fromCollection(Arrays.asList(new Tuple2("jiang",1L),new Tuple2<>("min",2L),new Tuple2<>("bing",3L)));

        /**
         * 执行窗口函数
         */
        input
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new MyProcessWindowFunction());
        /**
         * 具有聚合效果
         * 相同名字,年龄最大,在5s内,第一个
         */
        personDateStream.keyBy(p->p.getName()).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new MyReduceFunction2(),new MyProcessWindowFunction2());

        /**
         * 求相同类型的平均数
         */
        input
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new AverageAggregate3(),new MyProcessWindowFunction3());

        input
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(5))).trigger(CountTrigger.of(100));

        env2.execute();
    }
    /**
     * The accumulator is used to keep a running sum and a count. The {@code getResult} method
     * computes the average.
     */
    private static class AverageAggregate3
            implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
        }

        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            return ((double) accumulator.f0) / accumulator.f1;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    private static class MyProcessWindowFunction3
            extends ProcessWindowFunction<Double, Tuple2<String, Double>, String, TimeWindow> {

        public void process(String key,
                            Context context,
                            Iterable<Double> averages,
                            Collector<Tuple2<String, Double>> out) {
            Double average = averages.iterator().next();
            out.collect(new Tuple2<>(key, average));
        }
    }

    private static class MyReduceFunction2 implements ReduceFunction<Person> {

        public Person reduce(Person r1, Person r2) {
            return r1.getAge() > r2.getAge()? r2 : r1;
        }
    }

    private static class MyProcessWindowFunction2
            extends ProcessWindowFunction<Person, Tuple2<Long, Person>, String, TimeWindow> {

        public void process(String key,
                            Context context,
                            Iterable<Person> minReadings,
                            Collector<Tuple2<Long, Person>> out) {
            Person min = minReadings.iterator().next();
            out.collect(new Tuple2<Long, Person>(context.window().getStart(), min));
        }
    }
    public static class MyProcessWindowFunction
            extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Long>> input, Collector<String> out) {
            long count = 0;
            for (Tuple2<String, Long> in: input) {
                count++;
            }
            out.collect("Window: " + context.window() + "count: " + count);
        }
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
    public static class MyReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
            return new Tuple2<>(t1.f0 + "_" +t1.f0,t2.f1+t2.f1);
        }
    }
    public static class MyWindowFunction implements WindowFunction<Person, Integer, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow window, Iterable<Person> input, Collector<Integer> out) throws Exception {
            // 计算窗口之间的和
            int age = 0;
            for (Person p : input) {
                age += p.getAge();
            }
            out.collect(new Integer(age));
        }
    }

    /***
     * 聚合函数 求第二元素的平均值
     */
    private static class AverageAggregate
            implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
        }

        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            return ((double) accumulator.f0) / accumulator.f1;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

}
