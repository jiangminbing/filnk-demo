//package com.example;
//
//import com.example.entity.Alert;
//import com.example.entity.Transaction;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.util.Collector;
//
///**
// * @author jiangmb
// * @version 1.0.0
// * @date 2021-07-05 18:31
// */
//public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {
//
//    private static final long serialVersionUID = 1L;
//
//    private static final double SMALL_AMOUNT = 1.00;
//    private static final double LARGE_AMOUNT = 500.00;
//    private static final long ONE_MINUTE = 60 * 1000;
//
//    private transient ValueState<Boolean> flagState;
//    private transient ValueState<Long> timerState;
//
//    @Override
//    public void open(Configuration parameters) {
//        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
//                "flag",
//                Types.BOOLEAN);
//        flagState = getRuntimeContext().getState(flagDescriptor);
//
//        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
//                "timer-state",
//                Types.LONG);
//        timerState = getRuntimeContext().getState(timerDescriptor);
//    }
//
//    @Override
//    public void processElement(
//            Transaction transaction,
//            Context context,
//            Collector<Alert> collector) throws Exception {
//
//        // Get the current state for the current key
//        Boolean lastTransactionWasSmall = flagState.value();
//
//        // Check if the flag is set
//        if (lastTransactionWasSmall != null) {
//            if (transaction.getAmount() > LARGE_AMOUNT) {
//                //Output an alert downstream
//                Alert alert = new Alert();
//                alert.setId(transaction.getAccountId());
//
//                collector.collect(alert);
//            }
//            // Clean up our state
//            cleanUp(context);
//        }
//
//        // 如果交易
//        if (transaction.getAmount() < SMALL_AMOUNT) {
//            // set the flag to true
//            flagState.update(true);
//
//            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
//            context.timerService().registerProcessingTimeTimer(timer);
//
//            timerState.update(timer);
//        }
//    }
//
//    @Override
//    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
//        // remove flag after 1 minute
//        timerState.clear();
//        flagState.clear();
//    }
//
//    private void cleanUp(Context ctx) throws Exception {
//        // delete timer
//        Long timer = timerState.value();
//        ctx.timerService().deleteProcessingTimeTimer(timer);
//
//        // clean up all state
//        timerState.clear();
//        flagState.clear();
//    }
//
//}