//package com.example;
//
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.BroadcastStream;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.Arrays;
//
///**
// * @author jiangmb
// * @version 1.0.0
// * @date 2021-07-12 18:25
// */
//public class BroadcastStateDemo {
//    public static void main(String[] args) {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<Tuple2<String,String>> itemStream = env.fromCollection(Arrays.asList(new Tuple2("三角形","红色"),new Tuple2("长方形","黑色")));
//        // 将图形使用颜色进行划分
//        itemStream.keyBy(v->v.f0);
//        // 定义一个规制
//        MapStateDescriptor<String, String> ruleStateDescriptor = new MapStateDescriptor<>(
//                "RulesBroadcastState",
//                BasicTypeInfo.STRING_TYPE_INFO,
//                TypeInformation.of(new TypeHint<String>() {}));
//        // 广播流，广播规则并且创建 broadcast state
////        BroadcastStream<String> ruleBroadcastStream = ruleStream
//                .broadcast(ruleStateDescriptor);
//    }
//
//}
