package com.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import scala.Tuple1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author jiangmb
 * @version 1.0.0
 * @date 2021-07-05 20:36
 */
public class KafkaCustomer {

    public static void main(String[] args) throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // 非常关键，一定要设置启动检查点！！

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-group");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<>("tbox_period_A26",new SimpleStringSchema(),  props);

        DataStream<String> stream = env
                .addSource(consumer);
        SingleOutputStreamOperator<Tuple2<String,Integer>> operator = stream.flatMap(new LineSplitter()).
        keyBy(v->v.f0).sum(1).returns( TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class));
        operator.addSink(new MysqlSink());
        env.execute("Mydemo");
    }
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String v, Collector<Tuple2<String, Integer>> o) throws Exception {
            String str[] = v.split("\\|");
            for (int i = 0; i <str.length ; i++) {
                Tuple2<String,Integer> t = new Tuple2<>(str[i],1);
                o.collect(t);
            }
        }
    }
    public static final class MysqlSink extends RichSinkFunction<Tuple2<String,Integer>> {
        PreparedStatement ps;
        private Connection connection;

        /**
         * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            connection = getConnection();
            String sql = "insert into test(name, count) values(?, ?);";
            ps = this.connection.prepareStatement(sql);
        }

        @Override
        public void close() throws Exception {
            super.close();
            //关闭连接和释放资源
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        }

        /**
         * 每条数据的插入都要调用一次 invoke() 方法
         *
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(Tuple2<String,Integer> value, Context context) throws Exception {
            //组装数据，执行插入操作
            ps.setString(1, value.f0);
            ps.setInt(2,value.f1);
            ps.executeUpdate();
        }

        private static Connection getConnection() {
            Connection con = null;
            try {
                Class.forName("com.mysql.jdbc.Driver");
                con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "root123456");
            } catch (Exception e) {
                System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
            }
            return con;
        }
    }

}
