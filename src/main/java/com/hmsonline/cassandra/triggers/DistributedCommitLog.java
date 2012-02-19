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

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.thrift.Column;
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
import org.apache.commons.collections.MapUtils;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedCommitLog extends CassandraStore {
    private static Logger logger = LoggerFactory.getLogger(DistributedCommitLog.class);

    public static final String KEYSPACE = "triggers";
    public static final String COLUMN_FAMILY = "CommitLog";
    public static final int MAX_NUMBER_COLUMNS = 1000;
    public static final int BATCH_SIZE = 50;
    public static final int IN_FUTURE = 1000 * 60;
    private static DistributedCommitLog instance = null;

    private static Timer triggerTimer = null;
    private static final long TRIGGER_FREQUENCY = 5000; // every X milliseconds
    private static final long MAX_LOG_ENTRY_AGE = 5000; // age of entry, at
                                                        // which time any node
                                                        // can process it.
    private String hostName = null;

    public DistributedCommitLog(String keyspace, String columnFamily) throws Exception {
        super(keyspace, columnFamily);
        logger.debug("Instantiated distributed commit log.");
        this.getHostName();
        triggerTimer = new Timer(true);
        triggerTimer.schedule(new TriggerTask(), 0, TRIGGER_FREQUENCY);
        Log.debug("Started Trigger Task thread.");

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
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange(ByteBufferUtil.bytes(""), ByteBufferUtil.bytes(""), false, MAX_NUMBER_COLUMNS);
        predicate.setSlice_range(range);

        KeyRange keyRange = new KeyRange(BATCH_SIZE);
        keyRange.setStart_key(ByteBufferUtil.bytes(""));
        keyRange.setEnd_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        ColumnParent parent = new ColumnParent(COLUMN_FAMILY);
        List<KeySlice> rows = getConnection(KEYSPACE).get_range_slices(parent, predicate, keyRange,
                ConsistencyLevel.ALL);
        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        for (KeySlice keySlice : rows) {
            if (keySlice.columns.size() > 0) {
                LogEntry logEntry = new LogEntry();
                logEntry.setUuid(ByteBufferUtil.string(keySlice.key));
                for (ColumnOrSuperColumn cc : keySlice.columns) {
                    if (ByteBufferUtil.string(cc.column.name).equals("ks")) {
                        logEntry.setKeyspace(ByteBufferUtil.string(cc.column.value));
                    } else if (ByteBufferUtil.string(cc.column.name).equals("cf")) {
                        logEntry.setColumnFamily(ByteBufferUtil.string(cc.column.value));
                    } else if (ByteBufferUtil.string(cc.column.name).equals("row")) {
                        logEntry.setRowKey(cc.column.value);
                    } else if (ByteBufferUtil.string(cc.column.name).equals("status")) {
                        logEntry.setStatus(LogEntryStatus.valueOf(ByteBufferUtil.string(cc.column.value)));
                    } else if (ByteBufferUtil.string(cc.column.name).equals("timestamp")) {
                        logEntry.setTimestamp(Long.valueOf(ByteBufferUtil.string(cc.column.value)));
                    } else if (ByteBufferUtil.string(cc.column.name).equals("host")) {
                        logEntry.setHost(ByteBufferUtil.string(cc.column.value));
                    } else if (ConfigurationStore.getStore().shouldWriteColumns()){
                        ColumnOperation operation = new ColumnOperation();
                        operation.setName(cc.column.name);
                        operation.setOperationType(cc.column.value);
                    }
                }
                logEntries.add(logEntry);
            }
        }
        return logEntries;
    }

    public void writeLogEntry(LogEntry logEntry) throws Throwable {
        List<Mutation> slice = new ArrayList<Mutation>();
        slice.add(getMutation("ks", logEntry.getKeyspace()));
        slice.add(getMutation("cf", logEntry.getColumnFamily()));
        slice.add(getMutation("row", logEntry.getRowKey()));
        slice.add(getMutation("status", logEntry.getStatus().toString()));
        slice.add(getMutation("timestamp", Long.toString(logEntry.getTimestamp())));
        slice.add(getMutation("host", logEntry.getHost()));
        if (MapUtils.isNotEmpty(logEntry.getErrors())) {
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

}
