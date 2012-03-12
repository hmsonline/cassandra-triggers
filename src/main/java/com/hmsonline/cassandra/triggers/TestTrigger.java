package com.hmsonline.cassandra.triggers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTrigger implements Trigger {
    private static Logger logger = LoggerFactory.getLogger(TestTrigger.class);
    private static boolean wasCalled = false;

    public void process(LogEntry logEntry) {
        logger.debug("Trigger processing : [" + logEntry.getUuid() + "]");   
        wasCalled = true;
        throw new RuntimeException("Foo");
    }

    public static boolean wasCalled() {
        return wasCalled;
    }
}
