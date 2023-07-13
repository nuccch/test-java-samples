package org.chench.extra.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Properties;

/**
 * Flink application
 *
 * https://flink.apache.org/zh/
 *
 * @author chench
 * @desc org.chench.extra.flink.FlinkApp
 * @date 2022.11.04
 */
public class FlinkApp {
    public static void main(String[] args) throws Exception {
        // kafka source -> filter -> redis sink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.enableCheckpointing(2 * 60 * 1000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "testGroup");
        String topic = "testTopic";
        env
        // 设置数据源
        .addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties))
        .filter(s -> {
            // 过滤不标准格式的数据，并格式化
            String[] logs = s.split("\\\\t");
            if (!"clt".equals(logs[0])) {
                return false;
            }
            if (logs.length < 20) {
                return false;
            }
            int hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
            if (hour > 23) {
                return false;
            }
            if ("autospeed_ios".equals(logs[2]) || "autospeed_android".equals(logs[2])   // 极速版
                    || "auto_iphone".equals(logs[2]) || "auto_android".equals(logs[2])   // 主软
                    || "price_android".equals(logs[2]) || "price_ios".equals(logs[2])) { // 报价
                return true;
            } else {
                return false;
            }
        })
        .map(s -> {
            // 数据转换
            String[] logs = s.split("\\\\t");
            String deviceId = logs[6].toUpperCase();
            return deviceId;
        })
        .process(new ProcessFunction<String, String>() {
                 @Override
                 public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                     out.collect(value);
                 }
             }
        ).addSink(new RedisSink());
        env.execute("FlinkApp");
    }
}
