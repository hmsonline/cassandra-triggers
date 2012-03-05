package com.hmsonline.cassandra.triggers;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerExecutionThread implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(DistributedCommitLog.class);

    private BlockingQueue<LogEntry> workQueue = null;

    public TriggerExecutionThread(BlockingQueue<LogEntry> workQueue) {
        this.workQueue = workQueue;
    }

    public void run() {
        try {
            LogEntry logEntry = null;
            while (!((logEntry = workQueue.take()).equals("DONE"))) {
                try {
                    processLogEntry(logEntry);
                } catch (Throwable t) {
                    logger.debug("Error processing logEntries", t);
                }
            }
        } catch (InterruptedException intEx) {
            logger.debug("Trigger thread interrupted, closing the door on the way out.");
        }
    }

    protected void processLogEntry(LogEntry logEntry) throws Exception, Throwable {
        // Make sure its mine, or its old enough that I should pick
        // it up to ensure processing by someone
        if (DistributedCommitLog.getLog().isMine(logEntry) || DistributedCommitLog.getLog().isOld(logEntry)) {

            // Make sure it hadn't error'd previously
            if (!LogEntryStatus.ERROR.equals(logEntry.getStatus())) {
                logger.debug("Processing Entry [" + logEntry.getUuid() + "]:[" + logEntry.getKeyspace() + "]:["
                        + logEntry.getColumnFamily() + "]");
                String path = logEntry.getKeyspace() + ":" + logEntry.getColumnFamily();
                List<Trigger> triggers = TriggerStore.getStore().getTriggers().get(path);
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
                                        trigger.getClass().getName(), stackToString(t));
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

    protected static String stackToString(Throwable t) {
        Writer writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        t.printStackTrace(printWriter);
        if (t.getMessage() == null) {
            return writer.toString();
        } else {
            return t.getMessage() + "\n " + writer.toString();
        }
    }
}
