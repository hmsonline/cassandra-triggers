package com.hmsonline.cassandra.triggers;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitLog extends LogEntryStore {
    private static Logger logger = LoggerFactory.getLogger(CommitLog.class);

    public static final String KEYSPACE = "triggers";
    public static final String COLUMN_FAMILY_PREFIX = "CommitLog";
    public static final int MAX_NUMBER_COLUMNS = 100;
    public static final int BATCH_SIZE = 1000;

    private Thread triggerThread = null;
    private static final long MAX_LOG_ENTRY_AGE = 5000; // age of entry, at
                                                        // which time any node
                                                        // can process it.

    public CommitLog() throws Exception {
        super(KEYSPACE, COLUMN_FAMILY_PREFIX);
        logger.warn("Instantiated commit log [" + this.getColumnFamily() + "]");
        triggerThread = new Thread(new TriggerTask());
        triggerThread.start();
        logger.debug("Started Trigger Task thread.");
    }

    private static CommitLog instance = null;

    public static synchronized CommitLog getCommitLog() throws Exception {
        if (instance == null) {
            instance = new CommitLog();
        }
        return instance;
    }

    public List<LogEntry> writePending(ConsistencyLevel consistencyLevel, RowMutation rowMutation) throws Throwable {
    	List<String> columnNames = new ArrayList<String>();
    	for (ColumnFamily cf : rowMutation.getColumnFamilies()) {
    		for(ByteBuffer b : cf.getColumnNames()) {
    			columnNames.add(ByteBufferUtil.string(b));
    		}
    	}
        String keyspace = rowMutation.getTable();
        ByteBuffer rowKey = rowMutation.key();
        List<LogEntry> entries = new ArrayList<LogEntry>();
        for (Integer cfId : rowMutation.getColumnFamilyIds()) {
            ColumnFamily columnFamily = rowMutation.getColumnFamily(cfId);
            String path = keyspace + ":" + columnFamily.metadata().cfName;
            List<Trigger> triggers = TriggerStore.getStore().getTriggers().get(path);
            if (triggers != null && triggers.size() > 0) {
                String hostName = LogEntryStore.getHostName();
                LogEntry entry = new LogEntry(keyspace, columnFamily, rowKey, consistencyLevel, hostName,
                        System.currentTimeMillis(), columnNames);
                entries.add(entry);
                write(entry);
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
        ColumnParent parent = new ColumnParent(this.getColumnFamily());
        List<KeySlice> rows = getConnection(KEYSPACE).get_range_slices(parent, predicate, keyRange,
                ConsistencyLevel.ALL);
        result.addAll(toLogEntry(rows));
        return result;
    }

    public boolean isMine(LogEntry logEntry) throws UnknownHostException, SocketException {
        return (logEntry.getHost().equals(getHostName()));
    }

    public boolean isOld(LogEntry logEntry) {
        long now = System.currentTimeMillis();
        long age = now - logEntry.getTimestamp();
        return (age > CommitLog.MAX_LOG_ENTRY_AGE);
    }
}
