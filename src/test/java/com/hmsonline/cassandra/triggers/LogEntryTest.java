package com.hmsonline.cassandra.triggers;

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.json.simple.JSONValue;
import org.junit.Test;

/**
 * @author <a href=irieksts@healthmarketscience.com>Isaac Rieksts</a>
 * 
 */
public class LogEntryTest {

    @Test
    public void testToJson() throws CharacterCodingException {
        LogEntry le = new LogEntry();
        le.setConsistencyLevel(ConsistencyLevel.ONE);
        le.setRowKey(ByteBufferUtil.bytes("abc"));
        le.setStatus(LogEntryStatus.COMMITTED);
        List<String> columnNames = new ArrayList<String>();
        columnNames.add("col1");
        le.setColumnNames(columnNames);

        String json = JSONValue.toJSONString(le.toMap()).toString();

        LogEntry result = LogEntry.fromJson(json);
        Assert.assertEquals(le.getRowKey(), result.getRowKey());
        Assert.assertEquals(le.getColumnNames().iterator().next(), "col1");
    }
}
