package org.chench.extra.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.lang.StringUtils;

/**
 * Lettuce client
 *
 * https://lettuce.io/
 *
 * @author chench
 * @desc org.chench.extra.redis.LettuceClient
 * @date 2022.11.03
 */
public class LettuceClient {
    private String host = "localhost";
    private int port = 6379;
    private String auth = "";
    private int db = 0;

    public LettuceClient(String host, int port, String auth) {
        this.host = host;
        this.port = port;
        this.auth = auth;
    }

    public LettuceClient(String host, int port) {
        this(host, port, "");
    }

    public  LettuceClient() { }

    public Object get(String key) {
        Object value = null;
        try (RedisClient redisClient = getRedisClient()) {
            try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
                RedisCommands<String, String> syncCommands = connection.sync();
                value = syncCommands.get(key);
            }
        }
        return value;
    }

    public void set(String key, Object value, int expiredSeconds) {
        try (RedisClient redisClient = getRedisClient()) {
            try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
                RedisCommands<String, String> syncCommands = connection.sync();
                syncCommands.set(key, value.toString());
                syncCommands.expire(key, expiredSeconds);
            }
        }
    }

    private RedisClient getRedisClient() {
        return getRedisClient(this.host, this.port, this.auth, this.db);
    }

    private RedisClient getRedisClient(String host, int port, String auth, int db) {
        // redis://password@localhost:6379/0
        String schema = StringUtils.isEmpty(auth) ?
                String.format("redis://%s:%s/%s", host, port, db):
                String.format("redis://%s@%s:%s/%s", auth, host, port, db);
        RedisClient redisClient = RedisClient.create(schema);
        return redisClient;
    }
}
