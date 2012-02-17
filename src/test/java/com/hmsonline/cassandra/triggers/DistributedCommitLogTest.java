package com.hmsonline.cassandra.triggers;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DistributedCommitLogTest extends AbstractTriggerTest {
//    private static Logger logger = LoggerFactory.getLogger(DistributedCommitLogTest.class);

    @org.junit.Test
    public void testLogWrite() throws Throwable {
        DistributedCommitLog.getLog().getHostName();
        ColumnFamily columnFamily = ColumnFamily.create(AbstractTriggerTest.DATA_KEYSPACE, AbstractTriggerTest.DATA_CF1);
        ByteBuffer rowKey = ByteBufferUtil.bytes("testRow");
        String host = DistributedCommitLog.getLog().getHostName();        
        LogEntry logEntry = new LogEntry(AbstractTriggerTest.DATA_KEYSPACE, columnFamily, rowKey, ConsistencyLevel.ALL,
                host, System.currentTimeMillis());
        DistributedCommitLog.getLog().writeLogEntry(logEntry);     
        
        List<LogEntry> logEntries = DistributedCommitLog.getLog().getPending();
        assert(logEntries.size() == 1);
    }
}
