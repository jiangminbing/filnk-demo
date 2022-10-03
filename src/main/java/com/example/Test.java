package com.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * @author jiangmb
 * @version 1.0.0
 * @date 2022-09-14 01:00
 */
public class Test {
    public static void main(String[] args) throws Exception{
        long ct=System.currentTimeMillis();
        System.out.println(ct);// 打印我触发的时间
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source = e
                .addSource(new SourceFunction<Long>() {
                    private volatile boolean stop = false;
                    // 数据源是当前时间，一共有1000条数据
                    @Override
                    public void run(SourceContext<Long> ctx) throws Exception {
                        for(int i=0;i<200;i++){
                            ctx.collect(System.currentTimeMillis());
                        }
                    }

                    @Override
                    public void cancel() {
                        stop = true;
                    }
                }).setParallelism(1);
        e.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); // optional for Processing time
        source.keyBy(v->v/1000).process(new KeyedProcessFunction<Long, Long, Long>() {
            private ValueState<Integer> itemState;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<Integer> itemsStateDesc = new ValueStateDescriptor<>(
                        "itemState-state",
                        Integer.class);
                itemState = getRuntimeContext().getState(itemsStateDesc);
            }

            @Override
            public void processElement(Long value, Context ctx, Collector<Long> out) throws Exception {
                // 每条数据会存入state，并用同一个时间ct出发定时器。
                int val=(itemState.value()==null)?0:itemState.value();
                itemState.update(val+1);
                ctx.timerService().registerProcessingTimeTimer(ct);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Long> out) throws Exception {
                //出发定时器时打印state， 触发时间，key
                System.out.println(itemState.value());
                System.out.println(timestamp+"——"+ctx.getCurrentKey());
                System.out.println();
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

        }).setParallelism(1);
        e.execute();
    }
}
