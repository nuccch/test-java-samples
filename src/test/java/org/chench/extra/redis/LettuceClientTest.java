package org.chench.extra.redis;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Lettuce client tests
 *
 * @author chench
 * @desc org.chench.extra.redis.LettuceClientTest
 * @date 2022.11.03
 */
public class LettuceClientTest {
    private LettuceClient lettuceClient;
    private String key = "test";
    private Object value = "hello,world!";

    @Before
    public void setup() {
        String host = "localhost";
        int port = 6379;
        this.lettuceClient = new LettuceClient(host, port);
    }

    @Test
    public void testSet() {
        this.lettuceClient.set(this.key, this.value, 10);
    }

    @Test
    public void testGet() {
        Object value = this.lettuceClient.get(key);
        Assert.assertEquals(this.value, value);
    }
}
