package com.hmsonline.cassandra.triggers;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class IndexTrigger implements Trigger {

  private static Logger logger = LoggerFactory.getLogger(IndexTrigger.class);

  private Indexer indexer = null;

  public synchronized Indexer getIndexer(String solrUrl) {
    if (indexer == null) {
      indexer = new SolrIndexer(solrUrl);
    }
    return indexer;
  }

  public void process(LogEntry logEntry) {
    ConsistencyLevel consistencyLevel = logEntry.getConsistencyLevel();
    String keyspace = logEntry.getKeyspace();
    String columnFamily = logEntry.getColumnFamily();
    try {
      String rowKey = ByteBufferUtil.string(logEntry.getRowKey());// logEntry.getUuid();

      indexer = getIndexer(System.getProperty("solrHost"));
      logger.debug("solr enabled: " + indexer.toString());
      if (isMarkedForDelete(logEntry)) {
        indexer.delete(columnFamily, rowKey);
      }
      else {
        JSONObject json = DistributedCommitLog.getLog().getSlice(keyspace, columnFamily, rowKey, consistencyLevel);
        indexer.index(columnFamily, rowKey, json);
      }
    }
    catch (Exception ex) {
      throw new RuntimeException("Unable to update index", ex);
    }
  }

  //TODO - dq: migrate AOP to CassandraServerTriggerAspect and refactor
  @After("execution(* com.hmsonline.cassandra.triggers.DistributedCommitLog.writeLogEntry(..))")
  // @After("execution(* com.hmsonline.cassandra.triggers.CassandraServerTriggerAspect.writeCommitted(..))")
  public void indexToSolr(JoinPoint thisJoinPoint) throws Throwable {
    if (System.getProperty("solrHost") == null) {
      return;
    }

    LogEntry logEntry = (LogEntry) thisJoinPoint.getArgs()[0];
    if (logEntry.getStatus() == LogEntryStatus.COMMITTED) {
      process(logEntry);
    }
  }

  @AfterThrowing(pointcut = "execution(* com.hmsonline.cassandra.triggers.DistributedCommitLog.writeLogEntry(..))", throwing = "throwable")
  public void logErrorFromThrownException(final JoinPoint joinPoint, final Throwable throwable) {
    final String className = joinPoint.getTarget().getClass().getName();
    final String methodName = joinPoint.getSignature().getName();

    logger.error("Could not write committed log! Method: " + className + "." + methodName + "()", throwable);
  }

  //
  // PRIVATE METHODS
  //
  private boolean isMarkedForDelete(LogEntry logEntry) {
    boolean isDeletedEntry = false;
    for (ColumnOperation operation : logEntry.getOperations()) {
      if (operation.isDelete()) {
        isDeletedEntry = true;
        break;
      }
    }
    return isDeletedEntry;
  }

}
