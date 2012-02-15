package com.hmsonline.cassandra.triggers;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class CassandraServerTriggerAspect {
    private static Logger logger = LoggerFactory.getLogger(CassandraServerTriggerAspect.class);
    private static Timer triggerTimer = null;
    private static final long TRIGGER_FREQUENCY = 5000; // every X milliseconds

    static {
        triggerTimer = new Timer(true);
        triggerTimer.schedule(new TriggerTask(), 0, TRIGGER_FREQUENCY);
    }
    
    @Around("execution(* org.apache.cassandra.thrift.CassandraServer.doInsert(..))")
    public void writeToCommitLog(ProceedingJoinPoint thisJoinPoint) throws Throwable {
        if (ConfigurationStore.getStore().isCommitLogEnabled()) {
          List<LogEntry> logEntries = null;
            try {
                ConsistencyLevel consistencyLevel = (ConsistencyLevel) thisJoinPoint.getArgs()[0];
                @SuppressWarnings("unchecked")
                List<IMutation> mutations = (List<IMutation>) thisJoinPoint.getArgs()[1];
                logEntries = writePending(consistencyLevel, mutations);
                thisJoinPoint.proceed(thisJoinPoint.getArgs());
                writeCommitted(logEntries);
            }
            catch (InvalidRequestException e) {
              if(logEntries != null) {
                  for(LogEntry logEntry : logEntries) {
                      DistributedCommitLog.getLog().removeLogEntry(logEntry);
                  }
              }
            }
        } else {
            thisJoinPoint.proceed(thisJoinPoint.getArgs());
        }
    }

    /**
     * Logs an error message for unhandled exception thrown from the target method.
     * 
     * @param joinPoint - the joint point cut that contains information about the target
     * @param throwable - the cause of the exception from the target method invocation
     */
    @AfterThrowing(pointcut = "execution(* org.apache.cassandra.thrift.CassandraServer.doInsert(..))", throwing = "throwable")
    public void logErrorFromThrownException(final JoinPoint joinPoint, final Throwable throwable) {
        final String className = joinPoint.getTarget().getClass().getName();
        final String methodName = joinPoint.getSignature().getName();

        logger.error("Could not write to cassandra! Method: " + className + "."+ methodName + "()", throwable);
    }

    private List<LogEntry> writePending(ConsistencyLevel consistencyLevel, List<IMutation> mutations) throws Throwable {
        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        for (IMutation mutation : mutations) {
            if (mutation instanceof RowMutation) {
                RowMutation rowMutation = (RowMutation) mutation;
                logger.debug("Mutation for [" + rowMutation.getTable() + "] with consistencyLevel [" + consistencyLevel
                        + "]");
                if (!rowMutation.getTable().equals(DistributedCommitLog.KEYSPACE)) {
                    logEntries.addAll(DistributedCommitLog.getLog().writePending(consistencyLevel, rowMutation));
                }
            }
        }
        return logEntries;
    }

    private void writeCommitted(List<LogEntry> logEntries) throws Throwable {
        for (LogEntry logEntry : logEntries) {
            logEntry.setStatus(LogEntryStatus.COMMITTED);
            DistributedCommitLog.getLog().writeLogEntry(logEntry);
        }
    }

}
