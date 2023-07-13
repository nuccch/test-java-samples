package org.chench.extra.logback;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用logback日志组件
 *
 * @author chench
 * @desc org.chench.extra.logback.LogbackTest
 * @date 2022.11.01
 */
public class LogbackTest {
    private static final Logger logger = LoggerFactory.getLogger(LogbackTest.class);

    @Test
    public void testInfo() {
        logger.info("testInfo");
    }
}
