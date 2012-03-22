package com.hmsonline.cassandra.triggers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerTask implements Runnable {
    private static int THREAD_POOL_SIZE = 20;
    private static int MAX_QUEUE_SIZE = 500;
    private List<Thread> threadPool = new ArrayList<Thread>();
    private BlockingQueue<LogEntry> workQueue = null;
    private ProcessingManager processing;
    private static Logger logger = LoggerFactory.getLogger(TriggerTask.class);

    public TriggerTask() {
        processing = new ProcessingManager();
        workQueue = new ArrayBlockingQueue<LogEntry>(MAX_QUEUE_SIZE);
        // Spinning up new thread pool
        logger.debug("Spawning [" + THREAD_POOL_SIZE + "] threads for commit log processing.");
        for (int i = 0; i < THREAD_POOL_SIZE; i++) {
            TriggerExecutionThread runnable = new TriggerExecutionThread(workQueue, processing);
            Thread thread = new Thread(runnable);
            threadPool.add(thread);
            thread.start();
        }
    }

    public void run() {
        boolean gotUpdates = false;
        while (true) {
            gotUpdates = false;
            try {
                if (ConfigurationStore.getStore().isCommitLogEnabled()) {
                    List<LogEntry> logEntries = CommitLog.getCommitLog().getPending();
                    if (logger.isDebugEnabled() && logEntries != null) {
                        logger.debug("Processing [" + logEntries.size() + "] logEntries.");
                    }
                    for (LogEntry logEntry : logEntries) {
                        if (!processing.isAlreadyBeingProcessed(logEntry.getUuid())) {
                            gotUpdates = true;
                            workQueue.put(logEntry);
                            processing.add(logEntry.getUuid());
                        }
                    }
                } else {
                    logger.debug("Skipping trigger execution because commit log is disabled.");
                }
            } catch (Throwable t) {
                logger.warn("Could not execute triggers [" + t.getMessage() + "]");
                logger.debug("Cause for not executing triggers.", t);
            }
            if (!gotUpdates) {
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    logger.error("Couldn't sleep.", e);
                }
            }
        }
    }

}
