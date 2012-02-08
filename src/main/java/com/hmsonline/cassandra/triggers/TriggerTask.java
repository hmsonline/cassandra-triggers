package com.hmsonline.cassandra.triggers;

import java.util.List;
import java.util.Map;
import java.util.TimerTask;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerTask extends TimerTask {
  private static Logger logger = LoggerFactory.getLogger(TriggerTask.class);

  @Override
  public void run() {
    try {
      if (ConfigurationStore.getStore().isCommitLogEnabled()) {
        Map<String, List<Trigger>> triggerMap = null;
        logger.debug("Running triggers.");
        triggerMap = TriggerStore.getStore().getTriggers();
        List<LogEntry> logEntries = DistributedCommitLog.getLog().getPending();
        for (LogEntry logEntry : logEntries) {
          if (!LogEntryStatus.ERROR.equals(logEntry.getStatus())) {
            logger.debug("Processing Entry [" + logEntry.getUuid() + "]:["
                    + logEntry.getKeyspace()
                    + "]:["
                    + logEntry.getColumnFamily() + "]");
            String path = logEntry.getKeyspace() + ":" + logEntry.getColumnFamily();
            List<Trigger> triggers = triggerMap.get(path);
            if (triggers != null) {
              for (Trigger trigger : triggers) {
                try {
                  trigger.process(logEntry);
                }
                catch (Throwable t) {
                  logEntry.setStatus(LogEntryStatus.ERROR);
                  logEntry.getErrors().put(trigger.getClass().getName(),
                                           ExceptionUtils.getMessage(t) + " : "
                                                   + ExceptionUtils.getFullStackTrace(t));
                }
              }
            }
            if (LogEntryStatus.ERROR.equals(logEntry.getStatus())) {
              DistributedCommitLog.getLog().errorLogEntry(logEntry);
            }
            else {
              // Provided all processed properly, remove the logEntry
              DistributedCommitLog.getLog().removeLogEntry(logEntry);
            }

          }
        }
      }
      else {
        logger.debug("Skipping trigger execution because commit log is disabled.");
      }
    }
    catch (Throwable t) {
      logger.error("Could not execute triggers.", t);
    }
  }
}
