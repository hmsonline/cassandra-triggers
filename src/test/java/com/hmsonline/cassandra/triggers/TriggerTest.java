package com.hmsonline.cassandra.triggers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CassandraDaemon;
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

    public static final ByteBuffer COLUMN = ByteBufferUtil.bytes("2012-01-01_12:12:12");
    public static final ByteBuffer VALUE = ByteBufferUtil.bytes("{\"observation\":\"8\"}");

    private static Logger logger = LoggerFactory.getLogger(TriggerTest.class);

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
