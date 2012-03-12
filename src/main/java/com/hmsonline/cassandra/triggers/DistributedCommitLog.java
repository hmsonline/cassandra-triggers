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

import net.sf.json.JSONSerializer;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
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
    public static final String ERROR_COLUMN_FAMILY = "ErrorLog";
    public static final int MAX_NUMBER_COLUMNS = 100;
    public static final int BATCH_SIZE = 1000;

    private static DistributedCommitLog instance = null;
    private static CassandraStore errors = null;
    private Thread triggerThread = null;
    private static final long MAX_LOG_ENTRY_AGE = 5000; // age of entry, at
                                                        // which time any node
                                                        // can process it.
    private String hostName = null;

    public DistributedCommitLog(String keyspace, String columnFamily) throws Exception {
        super(keyspace, columnFamily, new String[] { LogEntryColumns.STATUS.toString(),
                LogEntryColumns.HOST.toString()});
        logger.warn("Instantiated distributed commit log.");
        this.getHostName();
        triggerThread = new Thread(new TriggerTask());
        triggerThread.start();
        logger.debug("Started Trigger Task thread.");
    }

    public static synchronized DistributedCommitLog getLog() throws Exception {
        if (instance == null) {
            instance = new DistributedCommitLog(KEYSPACE, COLUMN_FAMILY);
            getErrorLog();
        }
        return instance;
    }
    
    public static synchronized CassandraStore getErrorLog() throws Exception {
      if (errors == null) {
        errors = new CassandraStore(KEYSPACE, ERROR_COLUMN_FAMILY);
      }
      return errors;
  }

    public List<LogEntry> writePending(ConsistencyLevel consistencyLevel, RowMutation rowMutation) throws Throwable {
        String keyspace = rowMutation.getTable();
        ByteBuffer rowKey = rowMutation.key();
        List<LogEntry> entries = new ArrayList<LogEntry>();
        for (Integer cfId : rowMutation.getColumnFamilyIds()) {
            ColumnFamily columnFamily = rowMutation.getColumnFamily(cfId);
            String path = keyspace + ":" + columnFamily.metadata().cfName;
            List<Trigger> triggers = TriggerStore.getStore().getTriggers().get(path);
            if(triggers != null && triggers.size() > 0) {
                String hostName = this.getHostName();
                LogEntry entry = new LogEntry(keyspace, columnFamily, rowKey, consistencyLevel, hostName,
                                              System.currentTimeMillis());
                entries.add(entry);
                writeLogEntry(entry);
            }
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
        
        List<KeySlice> rows = getConnection(KEYSPACE).get_range_slices(parent, predicate, keyRange, ConsistencyLevel.ALL);

        result.addAll(toLogEntry(rows));
        return result;
    }

    public void writeLogEntry(LogEntry logEntry) throws Throwable {
      writeLogEntry(logEntry, COLUMN_FAMILY);
    }
    
    public void writeLogEntry(LogEntry logEntry, String columnFamily) throws Throwable {
        List<Mutation> slice = new ArrayList<Mutation>();
        slice.add(getMutation(logEntry.getUuid(), JSONSerializer.toJSON(logEntry.toMap()).toString()));

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
        cfMutations.put(columnFamily, slice);

        ByteBuffer rowKey = ByteBufferUtil.bytes(getKey());
        mutationMap.put(rowKey, cfMutations);
        getConnection(KEYSPACE).batch_mutate(mutationMap, logEntry.getConsistencyLevel());
    }

    public void removeLogEntry(LogEntry logEntry) throws Throwable {
        long deleteTime = System.currentTimeMillis() * 1000;
        ColumnPath path = new ColumnPath(COLUMN_FAMILY);
        path.setColumn(ByteBufferUtil.bytes(logEntry.getUuid()));
        getConnection(KEYSPACE)
                .remove(ByteBufferUtil.bytes(logEntry.getCommitLogRowKey()), path, deleteTime, ConsistencyLevel.ALL);
    }

    public void errorLogEntry(LogEntry logEntry) throws Throwable {
        logEntry.setConsistencyLevel(ConsistencyLevel.ALL);
        DistributedCommitLog.getLog().writeLogEntry(logEntry, ERROR_COLUMN_FAMILY);
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
        if (rows == null || rows.size() == 0) {
            return logEntries;
        }
        for (KeySlice keySlice : rows) {
            if (keySlice.columns.size() > 0) {
                for (ColumnOrSuperColumn cc : keySlice.columns) {
                    LogEntry logEntry = LogEntry.fromJson(ByteBufferUtil.string(cc.column.value));
                    if(logEntry != null) {
                        logEntry.setCommitLogRowKey(ByteBufferUtil.string(keySlice.key));
                        logEntry.setUuid(ByteBufferUtil.string(cc.column.name));
                        logEntries.add(logEntry);
                    }
                }
            }
        }
        return logEntries;
    }
    
    private String getKey() {
      return "" + getHoursSinceEpoch();
    }
    
    private static long getHoursSinceEpoch() {
      return System.currentTimeMillis() / (1000 * 1000 * 60);
    }
}
