package com.hmsonline.cassandra.triggers;

import junit.framework.Assert;

import org.junit.Test;

/**
 * @author <a href=irieksts@healthmarketscience.com>Isaac Rieksts</a>
 */
public class TriggerTaskTest {
    @Test
    public void testStackToStringNPE() {
        try {
            ((String) null).toString();
        } catch (Exception e) {
            Assert.assertTrue("Should throw a null pointer",
                    TriggerExecutionThread.stackToString(e).contains("java.lang.NullPointerException"));
        }
    }

    @Test
    public void testStackToStringNullStack() {
        try {
            Exception t = new Exception();
            t.setStackTrace(new StackTraceElement[] {});
            throw t;
        } catch (Exception e) {
            Assert.assertEquals("Should throw a null pointer", "java.lang.Exception\n",
                    TriggerExecutionThread.stackToString(e));
        }
    }
}
