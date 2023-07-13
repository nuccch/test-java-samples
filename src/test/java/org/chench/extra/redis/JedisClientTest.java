package org.chench.extra.redis;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Jedis client tests
 *
 * @author chench
 * @desc org.chench.extra.redis.JedisClientTest
 * @date 2022.11.03
 */
public class JedisClientTest {
    private JedisClient jedisClient;
    private String key = "test";
    private String value = "hello, world";

    @Before
    public void setup() {
        String host = "127.0.0.1";
        int port = 6379;
        this.jedisClient = new JedisClient(host, port);
    }

    @Test
    public void testSet() {
        this.jedisClient.set(this.key, this.value, 10);
    }

    @Test
    public void testGet() {
        Object value = this.jedisClient.get(this.key);
        Assert.assertEquals(this.value, value);
    }
}
