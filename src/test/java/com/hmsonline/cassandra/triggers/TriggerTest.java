package com.hmsonline.cassandra.triggers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerTest extends AbstractTriggerTest {
    private final static String CLUSTER_NAME = "TEST_CLUSTER";
    private final static String ROW_KEY = "TEST_ROW_KEY";
    
    public static final ByteBuffer COLUMN = ByteBufferUtil.bytes("2012-01-01_12:12:12");
    public static final ByteBuffer VALUE = ByteBufferUtil.bytes("{\"observation\":\"8\"}");

    private static Logger logger = LoggerFactory.getLogger(TriggerTest.class);

    
    @org.junit.Test
    public void testLogWrite() throws Throwable {
        TriggerStore.getStore().insertTrigger(DATA_KEYSPACE, DATA_CF1, 
                "com.hmsonline.cassandra.triggers.TestTrigger");
        
        Cluster cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, "localhost:9160");
        Map<String, String> columns = new HashMap<String,String>();
        columns.put("col1", "val1");
        this.persist(cluster, DATA_KEYSPACE, DATA_CF1, ROW_KEY, columns);
        List<LogEntry> logEntries = CommitLog.getCommitLog().getPending();
        Thread.sleep(100);
        assertTrue(logEntries.size() >= 1);
        Thread.currentThread();
        Thread.sleep(5000);
        logEntries = CommitLog.getCommitLog().getPending();
        assertEquals(0, logEntries.size());
        assertTrue(TestTrigger.wasCalled());
    }
    
    @org.junit.Test
    public void testThrowingExceptionWhenInsertingColumn() throws Throwable {
        TTransport tr = new TFramedTransport(new TSocket("localhost", 9160));
        TProtocol proto = new TBinaryProtocol(tr);
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();

        String rowkeyId = "PI123";
        long timestamp = System.currentTimeMillis();

        client.set_keyspace(DATA_KEYSPACE);
        ColumnParent parent = new ColumnParent(DATA_CF2);

        Column nameColumn = new Column(COLUMN);
        nameColumn.setValue(COLUMN);
        nameColumn.setTimestamp(timestamp);

        try {
            client.insert(ByteBufferUtil.bytes(rowkeyId), parent, nameColumn, ConsistencyLevel.ONE);
        } catch (org.apache.thrift.TApplicationException ex) {
            logger.warn("expected=" + ex.getClass());
            org.junit.Assert.assertEquals("Internal error processing insert", ex.getMessage());
        }

        tr.flush();
        tr.close();
    }

}
