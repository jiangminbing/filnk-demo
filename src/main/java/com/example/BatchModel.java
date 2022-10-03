package com.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jiangmb
 * @version 1.0.0
 * @date 2021-07-11 22:09
 */
public class BatchModel {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置为批处理,一般不这样操作,通过启动命令来完成
        env2.setRuntimeMode(RuntimeExecutionMode.BATCH);
    }
}
