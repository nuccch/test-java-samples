package org.chench.extra.redis;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Jedis client
 *
 * https://github.com/redis/jedis
 *
 * @author chench
 * @desc org.chench.extra.redis.JedisClient
 * @date 2022.11.03
 */
@Slf4j
public class JedisClient {
    private String host = "localhost";
    private int port = 6379;
    private String auth = "";

    public JedisClient(String host, int port, String auth) {
        this.host = host;
        this.port = port;
        this.auth = auth;
    }

    public JedisClient(String host, int port) {
        this(host, port, null);
    }

    public JedisClient() { }

    public Object get(String key) {
        Object value = null;
        try (Jedis jedis = getJedisPool()) {
            value = jedis.get(key);
            log.info("{}={}", key, value);
        }
        return value;
    }

    public void set(String key, Object value, int expiredSeconds) {
        try (Jedis jedis = getJedisPool()) {
            jedis.set(key, value.toString());
            jedis.expire(key, expiredSeconds);
        }
    }

    private Jedis getJedisPool() {
        return getJedisPool(this.host, this.port, this.auth);
    }

    private Jedis getJedisPool(String host, int port, String auth) {
        JedisPool pool = new JedisPool(host, port, null, auth);
        return pool.getResource();
    }
}
