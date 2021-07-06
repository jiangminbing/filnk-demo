package com.example;

import com.example.entity.Alert;
import com.example.entity.AlertSink;
import com.example.entity.Transaction;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author jiangmb
 * @version 1.0.0
 * @date 2021-07-05 18:30
 */
public class FlinkDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-group");
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<>("test1", new SimpleStringSchema(), props);
        DataStream<Transaction> transactions = env
                .addSource(consumer)
                .name("transactions");

//        DataStream<Alert> alerts = transactions
//                .keyBy(Transaction::getAccountId)
//                .process(new FraudDetector())
//                .name("fraud-detector");

        // 输出结果到sink
//        alerts
//                .addSink(new AlertSink())
//                .name("send-alerts");

        env.execute("Fraud Detection");

    }
    public static class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

        private static final long serialVersionUID = 1L;

        private static final double SMALL_AMOUNT = 1.00;
        private static final double LARGE_AMOUNT = 500.00;
        private static final long ONE_MINUTE = 60 * 1000;

        private transient ValueState<Boolean> flagState;
        private transient ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                    "flag",
                    Types.BOOLEAN);
            flagState = getRuntimeContext().getState(flagDescriptor);

            ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                    "timer-state",
                    Types.LONG);
            timerState = getRuntimeContext().getState(timerDescriptor);
        }

        @Override
        public void processElement(
                Transaction transaction,
                Context context,
                Collector<Alert> collector) throws Exception {

            // 判断最近的一笔交易是否是小额交易
            Boolean lastTransactionWasSmall = flagState.value();

            // Check if the flag is set
            if (lastTransactionWasSmall != null) {
                // 是小额交易 在设定的时间段内又出现了一笔大额交易 生产报警信息
                if (transaction.getAmount() > LARGE_AMOUNT) {
                    //Output an alert downstream
                    Alert alert = new Alert();
                    alert.setId(transaction.getAccountId());

                    collector.collect(alert);
                }
                // 已经报警 清除状态 需要重新判断
                cleanUp(context);
            }

            // 如果交易是小额度交易 更新判断状态
            if (transaction.getAmount() < SMALL_AMOUNT) {
                // set the flag to true
                flagState.update(true);
                // 注册一个定时器
                long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
                context.timerService().registerProcessingTimeTimer(timer);

                timerState.update(timer);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
            // 一分钟内不满足报警条件将状态清除
            timerState.clear();
            flagState.clear();
        }

        private void cleanUp(Context ctx) throws Exception {
            // delete timer
            Long timer = timerState.value();
            ctx.timerService().deleteProcessingTimeTimer(timer);

            // clean up all state
            timerState.clear();
            flagState.clear();
        }

    }
}
