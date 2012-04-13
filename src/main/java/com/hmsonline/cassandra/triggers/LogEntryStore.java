package com.hmsonline.cassandra.triggers;

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
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogEntryStore extends CassandraStore {
    private static Logger logger = LoggerFactory.getLogger(LogEntryStore.class);
    private static String hostName = null;

    protected LogEntryStore(String keyspace, String columnFamily) throws Exception {
        super(keyspace, columnFamily + "_" + getHostName());
    }

    public void write(LogEntry logEntry) throws Throwable {
        write(logEntry, this.getColumnFamily());
    }

    public void write(LogEntry logEntry, String columnFamily) throws Throwable {
        List<Mutation> slice = new ArrayList<Mutation>();
        slice.add(getMutation(logEntry.getUuid(), JSONValue.toJSONString(logEntry.toMap()).toString()));

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
        getConnection(this.getKeyspace()).batch_mutate(mutationMap, logEntry.getConsistencyLevel());
    }

    public void remove(LogEntry logEntry) throws Throwable {
        long deleteTime = System.currentTimeMillis() * 1000;
        ColumnPath path = new ColumnPath(this.getColumnFamily());
        path.setColumn(ByteBufferUtil.bytes(logEntry.getUuid()));
        getConnection(this.getKeyspace()).remove(ByteBufferUtil.bytes(logEntry.getCommitLogRowKey()), path, deleteTime,
                ConsistencyLevel.ALL);
    }

    public static String getHostName() throws SocketException {
        if (hostName == null) {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
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
            hostName = hostName.replace(".", "_");
        }
        return hostName;
    }

    private static String getKey() {
        long hours = System.currentTimeMillis() / (1000 * 1000 * 60);
        return "" + hours;
    }

    public static List<LogEntry> toLogEntry(List<KeySlice> rows) throws Exception, Throwable {
        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        if (rows == null || rows.size() == 0) {
            return logEntries;
        }
        for (KeySlice keySlice : rows) {
            if (keySlice.columns.size() > 0) {
                for (ColumnOrSuperColumn cc : keySlice.columns) {
                    LogEntry logEntry = LogEntry.fromJson(ByteBufferUtil.string(cc.column.value));
                    if (logEntry != null) {
                        logEntry.setCommitLogRowKey(ByteBufferUtil.string(keySlice.key));
                        logEntry.setUuid(ByteBufferUtil.string(cc.column.name));
                        logEntries.add(logEntry);
                    }
                }
            }
        }
        return logEntries;
    }
}
