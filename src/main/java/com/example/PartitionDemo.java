package com.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author jiangmb
 * @version 1.0.0
 * @date 2021-07-10 22:25
 */
public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //对数据进行转换，把long类型转成tuple1类型
        DataStream<Tuple1<Long>> tupData = env.fromCollection(Arrays.asList(new Tuple1<>(1L),
                new Tuple1<>(3L),new Tuple1<>(2L),new Tuple1<>(5L)));

        //分区之后的数据
        DataStream<Tuple1<Long>> partitionData = tupData.partitionCustom(new MyPartition(), 0);

        DataStream<Long> result = partitionData.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("当前线程id" + Thread.currentThread().getId() + ",value" + value);
                return value.getField(0);
            }
        });
        result.print();
        // 随机分区
        tupData.shuffle();
        // 均匀分区
        tupData.rebalance();
        // 策重分区
        //基于上下游Operator的并行度，将记录以循环的方式输出到下游Operator的每个实例。举例: 上游并行度是2，下游是4，则上游一个并行度以循环的方式将记录输出到下游的两个并行度上;上游另一个并行度以循环的方式将记录输出到下游另两个并行度上。若上游并行度是4，下游并行度是2，则上游两个并行度将记录输出到下游一个并行度上；上游另两个并行度将记录输出到下游另一个并行度上。
        tupData.rescale();
        // 广播分区广播分区将上游数据集输出到下游Operator的每个实例中。适合于大数据集Join小数据集的场景。
        tupData.broadcast();
        //将记录输出到下游本地的operator实例。ForwardPartitioner分区器要求上下游算子并行度一样。
        tupData.forward();
        // KeyGroupStreamPartitioner,HASH分区。将记录按Key的Hash值输出到下游Operator实例。
        env.execute("StreamingDemoMyPartition");

    }
    public static class MyPartition implements Partitioner<Long> {

        @Override
        public int partition(Long key, int numPartition) {
//            System.out.println("分区总数："+numPartition);?
            if(key % 2 ==0){
                return 0;
            }else{
                return 1;
            }
        }
    }
}
