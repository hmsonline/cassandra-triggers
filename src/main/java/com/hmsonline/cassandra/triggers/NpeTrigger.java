//
// Copyright (c) 2012 Health Market Science, Inc.
//
package com.hmsonline.cassandra.triggers;

import java.nio.charset.CharacterCodingException;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NpeTrigger implements Trigger {
  private static Logger logger = LoggerFactory.getLogger(NpeTrigger.class);

  @SuppressWarnings("null")
  public void process(LogEntry logEntry) {
    try {
      logger.debug("Trigger about to throw NPE: " + ByteBufferUtil.string(logEntry.getRowKey()));
    }
    catch (CharacterCodingException e) {
      throw new RuntimeException(e);
    }
    String s = null;
    s.trim();
  }
}
