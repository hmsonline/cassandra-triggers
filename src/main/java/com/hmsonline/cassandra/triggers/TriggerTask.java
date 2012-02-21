package com.hmsonline.cassandra.triggers;

import java.util.List;
import java.util.Map;
import java.util.TimerTask;

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

                    // Make sure its mine, or its old enough that I should pick
                    // it up to ensure processing by someone
                    if (DistributedCommitLog.getLog().isMine(logEntry) || DistributedCommitLog.getLog().isOld(logEntry)) {

                        // Make sure it hadn't error'd previously
                        if (!LogEntryStatus.ERROR.equals(logEntry.getStatus())) {
                            logger.debug("Processing Entry [" + logEntry.getUuid() + "]:[" + logEntry.getKeyspace()
                                    + "]:[" + logEntry.getColumnFamily() + "]");
                            String path = logEntry.getKeyspace() + ":" + logEntry.getColumnFamily();
                            List<Trigger> triggers = triggerMap.get(path);
                            if (triggers != null && triggers.size() > 0 && triggers.get(0) instanceof PausedTrigger) {
                                logger.debug("Paused triggers for: " + logEntry.getColumnFamily());
                            } else {
                                if (triggers != null) {
                                    for (Trigger trigger : triggers) {
                                        try {
                                            trigger.process(logEntry);
                                        } catch (Throwable t) {
                                            logEntry.setStatus(LogEntryStatus.ERROR);
                                            logEntry.getErrors().put(                                                    
                                                    // TODO : Make this a stack trace
                                                    trigger.getClass().getName(), t.getMessage());
                                        }
                                    }
                                }
                                if (LogEntryStatus.ERROR.equals(logEntry.getStatus())) {
                                    DistributedCommitLog.getLog().errorLogEntry(logEntry);
                                } else {
                                    // Provided all processed properly, remove
                                    // the logEntry
                                    DistributedCommitLog.getLog().removeLogEntry(logEntry);
                                }
                            }
                        }
                    }
                }
            } else {
                logger.debug("Skipping trigger execution because commit log is disabled.");
            }
        } catch (Throwable t) {
            logger.error("Could not execute triggers.", t);
        }
    }
}
