//
// Copyright (c) 2012 Health Market Science, Inc.
//
package com.hmsonline.cassandra.triggers;

import java.nio.charset.CharacterCodingException;

import junit.framework.Assert;

import net.sf.json.JSONSerializer;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

/**
 * @author <a href=irieksts@healthmarketscience.com>Isaac Rieksts</a>
 *
 */
public class LogEntryTest {
  
  @Test
  public void testToJson() throws CharacterCodingException {
    LogEntry le = new LogEntry();
    le.setRowKey(ByteBufferUtil.bytes("abc"));
    le.setStatus(LogEntryStatus.COMMITTED);
    
    String json = JSONSerializer.toJSON(le.toMap()).toString();
    
    LogEntry result = LogEntry.fromJson(json);
    Assert.assertEquals(le.getRowKey(), result.getRowKey());
  }
}
