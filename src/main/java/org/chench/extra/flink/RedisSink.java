package org.chench.extra.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author chench
 * @desc org.chench.extra.flink.RedisSink
 * @date 2022.11.04
 */
public class RedisSink extends RichSinkFunction<String> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        // to do something
    }
}
