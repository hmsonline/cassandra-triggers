package com.hmsonline.cassandra.triggers.dao;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.cassandra.triggers.ColumnOperation;
import com.hmsonline.cassandra.triggers.LogEntry;
import com.hmsonline.cassandra.triggers.OperationType;

public abstract class LogEntryStore extends CassandraStore {
    private static Logger logger = LoggerFactory.getLogger(LogEntryStore.class);
    private static String hostName = null;
    private static final int DELETE_PRIORITY = 3;
    private static final String INVALID_CF_NAME_CHAR = "[^a-zA-Z0-9_]";

    protected LogEntryStore(String keyspace, String columnFamily) throws Exception {
        super(keyspace, columnFamily + "_" + getHostName());
    }

    public void write(LogEntry logEntry) throws Throwable {
        write(logEntry, this.getColumnFamily(), DEFAULT_PRIORITY);
    }

    public void write(LogEntry logEntry, int priority) throws Throwable {
        write(logEntry, this.getColumnFamily(), priority);
    }

    public void write(LogEntry logEntry, String columnFamily, int priority) throws Throwable {
        List<Mutation> slice = new ArrayList<Mutation>();
        slice.add(getMutation(logEntry.getUuid(), JSONValue.toJSONString(logEntry.toMap()).toString(), priority));

        if (ConfigurationStore.getStore().shouldWriteColumns()) {
            for (ColumnOperation operation : logEntry.getOperations()) {
                if (operation.isDelete()) {
                    slice.add(getMutation(operation.getName(), OperationType.DELETE, DELETE_PRIORITY));
                } else {
                    slice.add(getMutation(operation.getName(), OperationType.UPDATE, priority));
                }
            }
        }
        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        Map<String, List<Mutation>> cfMutations = new HashMap<String, List<Mutation>>();
        cfMutations.put(columnFamily, slice);

        ByteBuffer rowKey = ByteBufferUtil.bytes(getKey());
        mutationMap.put(rowKey, cfMutations);
        getConnection(this.getKeyspace()).batch_mutate(mutationMap, logEntry.getConsistencyLevel());
    }

    public void remove(LogEntry logEntry) throws Throwable {
        long deleteTime = System.currentTimeMillis() * 1000;
        ColumnPath path = new ColumnPath(this.getColumnFamily());
        path.setColumn(ByteBufferUtil.bytes(logEntry.getUuid()));
        getConnection(this.getKeyspace()).remove(ByteBufferUtil.bytes(logEntry.getCommitLogRowKey()), path, deleteTime,
                READ_CONSISTENCY);
    }

    public static String getHostName() throws SocketException {
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
            hostName = hostName.replaceAll(INVALID_CF_NAME_CHAR, "_");
        }
        return hostName;
    }

    public static String getKey() {
        long hours = System.currentTimeMillis() / (1000 * 1000 * 60);
        return "" + hours;
    }
    
    public static String getPreviousKey() {
        long hours = (System.currentTimeMillis() / (1000 * 1000 * 60)) - 1;
        return "" + hours;
    }

    public static List<LogEntry> toLogEntry(String rowKey, List<ColumnOrSuperColumn> columns) throws Exception, Throwable {
        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        if (columns == null || columns.size() == 0) {
            return logEntries;
        }
        for (ColumnOrSuperColumn cc : columns) {
            LogEntry logEntry = LogEntry.fromJson(ByteBufferUtil.string(cc.column.value));
            if (logEntry != null) {
                logEntry.setCommitLogRowKey(rowKey);
                logEntry.setUuid(ByteBufferUtil.string(cc.column.name));
                logEntries.add(logEntry);
            }
        }
        return logEntries;
    }
}
