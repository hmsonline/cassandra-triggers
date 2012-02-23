package com.hmsonline.cassandra.triggers;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedCommitLog extends CassandraStore {
    private static Logger logger = LoggerFactory.getLogger(DistributedCommitLog.class);

    public static final String KEYSPACE = "triggers";
    public static final String COLUMN_FAMILY = "CommitLog";
    public static final int MAX_NUMBER_COLUMNS = 1000;
    public static final int BATCH_SIZE = 50;
    
    // This is the time in seconds before this host will process messages from other hosts.
    public static final int TIME_BEFORE_PROCESS_OTHER_HOST = 20;  
    public static final int IN_FUTURE = 1000 * 60;
    private static DistributedCommitLog instance = null;

    private static Timer triggerTimer = null;
    private static final long TRIGGER_FREQUENCY = 5000; // every X milliseconds
    private static final long MAX_LOG_ENTRY_AGE = 5000; // age of entry, at
                                                        // which time any node
                                                        // can process it.
    private String hostName = null;

    public DistributedCommitLog(String keyspace, String columnFamily) throws Exception {
        super(keyspace, columnFamily, new String [] {LogEntryColumns.status.toString(), LogEntryColumns.host.toString(), LogEntryColumns.timestamp.toString()});
        logger.warn("Instantiated distributed commit log.");
        this.getHostName();
        triggerTimer = new Timer(true);
        triggerTimer.schedule(new TriggerTask(), 0, TRIGGER_FREQUENCY);
        logger.debug("Started Trigger Task thread.");
    }

    public static synchronized DistributedCommitLog getLog() throws Exception {
        if (instance == null) {
            instance = new DistributedCommitLog(KEYSPACE, COLUMN_FAMILY);
        }
        return instance;
    }

    public List<LogEntry> writePending(ConsistencyLevel consistencyLevel, RowMutation rowMutation) throws Throwable {
        String keyspace = rowMutation.getTable();
        ByteBuffer rowKey = rowMutation.key();
        List<LogEntry> entries = new ArrayList<LogEntry>();
        for (Integer cfId : rowMutation.getColumnFamilyIds()) {
            ColumnFamily columnFamily = rowMutation.getColumnFamily(cfId);
            String hostName = this.getHostName();
            LogEntry entry = new LogEntry(keyspace, columnFamily, rowKey, consistencyLevel, hostName,
                    System.currentTimeMillis());
            entries.add(entry);
            writeLogEntry(entry);
        }
        return entries;
    }

    public List<LogEntry> getPending() throws Throwable {
        List<LogEntry> result = new ArrayList<LogEntry>();
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange(ByteBufferUtil.bytes(""), ByteBufferUtil.bytes(""), false, MAX_NUMBER_COLUMNS);
        predicate.setSlice_range(range);

        KeyRange keyRange = new KeyRange(BATCH_SIZE);
        keyRange.setStart_key(ByteBufferUtil.bytes(""));
        keyRange.setEnd_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        ColumnParent parent = new ColumnParent(COLUMN_FAMILY);
        
        IndexClause indexClause = new IndexClause();
        indexClause.setCount(BATCH_SIZE);
        indexClause.setStart_key(new byte[0]);
        StringSerializer se = new StringSerializer();
        indexClause.addToExpressions(new IndexExpression(se.toByteBuffer(LogEntryColumns.status.toString()), IndexOperator.EQ, 
                                                         se.toByteBuffer(LogEntryStatus.COMMITTED.toString())));
        
        indexClause.addToExpressions(new IndexExpression(se.toByteBuffer(LogEntryColumns.host.toString()), IndexOperator.EQ, 
                                                         se.toByteBuffer(this.getHostName())));
        
        List<KeySlice> rows = getConnection(KEYSPACE).get_indexed_slices(parent, indexClause, predicate,
                                                                       ConsistencyLevel.ALL);
        
        result.addAll(toLogEntry(rows));
        
        indexClause = new IndexClause();
        indexClause.setCount(BATCH_SIZE);
        indexClause.setStart_key(new byte[0]);
        LongSerializer le = new LongSerializer();
        indexClause.addToExpressions(new IndexExpression(se.toByteBuffer(LogEntryColumns.status.toString()), IndexOperator.EQ, 
                                                         se.toByteBuffer(LogEntryStatus.COMMITTED.toString())));
        
        indexClause.addToExpressions(new IndexExpression(se.toByteBuffer((LogEntryColumns.timestamp.toString())), IndexOperator.LT, 
                                                         le.toByteBuffer(System.currentTimeMillis() - (1000L * TIME_BEFORE_PROCESS_OTHER_HOST))));
        
        rows = getConnection(KEYSPACE).get_indexed_slices(parent, indexClause, predicate,
                                                                       ConsistencyLevel.ALL);
        
        result.addAll(toLogEntry(rows));
        return result;
    }

    public void writeLogEntry(LogEntry logEntry) throws Throwable {
        List<Mutation> slice = new ArrayList<Mutation>();
        slice.add(getMutation(LogEntryColumns.ks.toString(), logEntry.getKeyspace()));
        slice.add(getMutation(LogEntryColumns.cf.toString(), logEntry.getColumnFamily()));
        slice.add(getMutation(LogEntryColumns.row.toString(), logEntry.getRowKey()));
        slice.add(getMutation(LogEntryColumns.status.toString(), logEntry.getStatus().toString()));
        slice.add(getMutation(LogEntryColumns.timestamp.toString(), Long.toString(logEntry.getTimestamp())));
        slice.add(getMutation(LogEntryColumns.host.toString(), logEntry.getHost()));
        if (logEntry.hasErrors()) {
            for (String errorKey : logEntry.getErrors().keySet()) {
                slice.add(getMutation(errorKey, logEntry.getErrors().get(errorKey)));
            }
        }

        if (ConfigurationStore.getStore().shouldWriteColumns()) {
            for (ColumnOperation operation : logEntry.getOperations()) {
                if (operation.isDelete()) {
                    slice.add(getMutation(operation.getName(), OperationType.DELETE));
                } else {
                    slice.add(getMutation(operation.getName(), OperationType.UPDATE));
                }
            }
        }
        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        Map<String, List<Mutation>> cfMutations = new HashMap<String, List<Mutation>>();
        cfMutations.put(COLUMN_FAMILY, slice);

        ByteBuffer rowKey = ByteBufferUtil.bytes(logEntry.getUuid());
        mutationMap.put(rowKey, cfMutations);
        getConnection(KEYSPACE).batch_mutate(mutationMap, logEntry.getConsistencyLevel());
    }

    public void removeLogEntry(LogEntry logEntry) throws Throwable {
        long deleteTime = System.currentTimeMillis() * 1000;
        ColumnPath path = new ColumnPath(COLUMN_FAMILY);
        getConnection(KEYSPACE)
                .remove(ByteBufferUtil.bytes(logEntry.getUuid()), path, deleteTime, ConsistencyLevel.ALL);
    }

    public void errorLogEntry(LogEntry logEntry) throws Throwable {
        logEntry.setConsistencyLevel(ConsistencyLevel.ALL);
        DistributedCommitLog.getLog().writeLogEntry(logEntry);
    }

    // Utility Methods
    private Mutation getMutation(String name, String value) {
        return getMutation(name, ByteBufferUtil.bytes(value));
    }

    private Mutation getMutation(String name, ByteBuffer value) {
        return getMutation(ByteBufferUtil.bytes(name), value);
    }

    private Mutation getMutation(ByteBuffer name, OperationType value) {
        return getMutation(name, ByteBufferUtil.bytes(value.toString()));
    }

    private Mutation getMutation(ByteBuffer name, ByteBuffer value) {
        Column c = new Column();
        c.setName(name);
        c.setValue(value);
        c.setTimestamp(System.currentTimeMillis() * 1000);

        Mutation m = new Mutation();
        ColumnOrSuperColumn cc = new ColumnOrSuperColumn();
        cc.setColumn(c);
        m.setColumn_or_supercolumn(cc);
        return m;
    }

    public String getHostName() throws SocketException {
        if (hostName == null) {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            {
                while (interfaces.hasMoreElements()) {
                    NetworkInterface nic = interfaces.nextElement();
                    Enumeration<InetAddress> addresses = nic.getInetAddresses();
                    while (hostName == null && addresses.hasMoreElements()) {
                        InetAddress address = addresses.nextElement();
                        if (!address.isLoopbackAddress()) {
                            hostName = address.getHostName();
                            logger.debug("Host ID: " + hostName);
                        }
                    }
                }
            }
        }
        return this.hostName;
    }

    public boolean isMine(LogEntry logEntry) throws UnknownHostException, SocketException {
        return (logEntry.getHost().equals(this.getHostName()));
    }

    public boolean isOld(LogEntry logEntry) {
        long now = System.currentTimeMillis();
        long age = now - logEntry.getTimestamp();
        return (age > DistributedCommitLog.MAX_LOG_ENTRY_AGE);
    }
    
    private static List<LogEntry> toLogEntry(List<KeySlice> rows) throws Exception, Throwable {
      List<LogEntry> logEntries = new ArrayList<LogEntry>();
      if(rows == null || rows.size() == 0) {
        return logEntries; 
      }
      for (KeySlice keySlice : rows) {
          if (keySlice.columns.size() > 0) {
              LogEntry logEntry = new LogEntry();
              logEntry.setUuid(ByteBufferUtil.string(keySlice.key));
              for (ColumnOrSuperColumn cc : keySlice.columns) {
                  if (ConfigurationStore.getStore().shouldWriteColumns()) {
                      ColumnOperation operation = new ColumnOperation();
                      operation.setName(cc.column.name);
                      operation.setOperationType(cc.column.value);
                  }
                  else {
                      switch (LogEntryColumns.valueOf(ByteBufferUtil.string(cc.column.name))) {
                        case ks:
                          logEntry.setKeyspace(ByteBufferUtil.string(cc.column.value));
                          break;
                        case cf:
                          logEntry.setColumnFamily(ByteBufferUtil.string(cc.column.value));
                          break;
                        case row:
                          logEntry.setRowKey(cc.column.value);
                          break;
                        case status:
                          logEntry.setStatus(LogEntryStatus.valueOf(ByteBufferUtil.string(cc.column.value)));
                          break;
                        case timestamp:
                          logEntry.setTimestamp(Long.valueOf(ByteBufferUtil.string(cc.column.value)));
                          break;
                        case host:
                          logEntry.setHost(ByteBufferUtil.string(cc.column.value));
                          break;
                      }
                    }
                }
                logEntries.add(logEntry);
              }
      }
      return logEntries;
    }

}
