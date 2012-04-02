package com.hmsonline.cassandra.triggers;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.json.simple.JSONValue;

public class LogEntry {
    private String commitLogRowKey = null;
    private String keyspace = null;
    private String columnFamily = null;
    private ConsistencyLevel consistencyLevel = null;
    private List<ColumnOperation> operations = new ArrayList<ColumnOperation>();
    private Collection<String> columnNames = new ArrayList<String>();
    private LogEntryStatus status = null;
    private ByteBuffer rowKey = null;
    private String uuid = null;
    private Map<String, String> errors = new HashMap<String, String>();
    private String host = null;
    
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    private long timestamp = -1;

    public LogEntry() {
        this.setConsistencyLevel(ConsistencyLevel.ALL);        
    }

    public LogEntry(String keyspace, ColumnFamily columnFamily, ByteBuffer rowKey, ConsistencyLevel consistencyLevel,
            String host, long timestamp, Collection<String> columnNames)
            throws Throwable {
        this.columnFamily = columnFamily.metadata().cfName;
        this.keyspace = keyspace;
        this.rowKey = rowKey;
        for (IColumn column : columnFamily.getSortedColumns()) {
            ColumnOperation operation = new ColumnOperation();
            operation.setName(column.name());
            operation.setDelete(columnFamily.isMarkedForDelete());
            operations.add(operation);
        }
        this.uuid = UUIDGen.getUUID(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())).toString();
        this.status = LogEntryStatus.PREPARING;
        this.consistencyLevel = consistencyLevel;
        this.timestamp = timestamp;
        this.host = host;
        this.columnNames = columnNames;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public List<ColumnOperation> getOperations() {
        return operations;
    }

    public void setOperations(List<ColumnOperation> operations) {
        this.operations = operations;
    }

    public void addOperation(ColumnOperation operation) {
        operations.add(operation);
    }

    public LogEntryStatus getStatus() {
        return status;
    }

    public void setStatus(LogEntryStatus status) {
        this.status = status;
    }

    public ByteBuffer getRowKey() {
        return rowKey;
    }

    public void setRowKey(ByteBuffer rowKey) {
        this.rowKey = rowKey;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    public Map<String, String> getErrors() {
      return errors;
    }

    public void setErrors(Map<String, String> errors) {
      this.errors = errors;
    }
    public boolean hasErrors(){
        return (this.getErrors() != null && this.getErrors().size() > 0);
    }
    
    public Map<String, Object> toMap() throws CharacterCodingException {
      HashMap<String, Object> result = new HashMap<String, Object>();
      result.put(LogEntryColumns.ROW.toString(), ByteBufferUtil.string(this.rowKey));
      result.put(LogEntryColumns.HOST.toString(), this.host);
      result.put(LogEntryColumns.KS.toString(), this.keyspace);
      result.put(LogEntryColumns.CF.toString(), this.columnFamily);
      result.put(LogEntryColumns.STATUS.toString(), "" + this.status);
      result.put(LogEntryColumns.TIMESTAMP.toString(), "" + this.timestamp);
      result.put(LogEntryColumns.COLUMN_NAMES.toString(), this.columnNames);
      if(this.errors != null) {
        result.putAll(this.errors);
      }
      return result;
    }
    
    @SuppressWarnings("unchecked")
	public static LogEntry fromJson(String json) throws CharacterCodingException {
      if(json == null || "".equals(json.trim())) {
        return null;
      }
      LogEntry result = new LogEntry();
      
      Map<String, Object> jsonObj = (Map<String, Object>) JSONValue.parse(json);
      result.setRowKey(ByteBufferUtil.bytes((String) jsonObj.get(LogEntryColumns.ROW.toString())));
      result.setKeyspace((String) jsonObj.get(LogEntryColumns.KS.toString()));
      result.setColumnFamily((String) jsonObj.get(LogEntryColumns.CF.toString()));
      result.setHost((String) jsonObj.get(LogEntryColumns.HOST.toString()));
      result.setStatus(LogEntryStatus.valueOf((String) (jsonObj.get(LogEntryColumns.STATUS.toString()))));
      result.setTimestamp(Long.parseLong((String) (jsonObj.get(LogEntryColumns.TIMESTAMP.toString()))));
      result.setColumnNames((List<String>) (jsonObj.get(LogEntryColumns.COLUMN_NAMES.toString())));
      return result;
    }

    /**
     * @return the commitLogRowKey
     */
    public String getCommitLogRowKey() {
      return commitLogRowKey;
    }

    /**
     * @param commitLogRowKey the commitLogRowKey to set
     */
    public void setCommitLogRowKey(String commitLogRowKey) {
      this.commitLogRowKey = commitLogRowKey;
    }

	/**
	 * @return the columnNames
	 */
	public Collection<String> getColumnNames() {
		return columnNames;
	}

	/**
	 * @param columnNames the columnNames to set
	 */
	public void setColumnNames(Collection<String> columnNames) {
		this.columnNames = columnNames;
	}
}
