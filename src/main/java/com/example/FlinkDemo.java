//package com.example;
//
//import javafx.scene.control.Alert;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.Transaction;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//
//import java.util.Properties;
//
///**
// * @author jiangmb
// * @version 1.0.0
// * @date 2021-07-05 17:25
// */
//public class FlinkDemo {
//    public static void main(String[] args) {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000); // 非常关键，一定要设置启动检查点！！
//
//        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", "localhost:9092");
//        props.setProperty("group.id", "flink-group");
//        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), props);
//        DataStream<String> stream = env
//                .addSource(consumer);
//    }
//}
