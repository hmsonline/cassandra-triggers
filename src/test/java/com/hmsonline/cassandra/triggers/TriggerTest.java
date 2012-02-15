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

public class TriggerTest {

  public static final String CASSANDRA_HOST = "localhost";
  public static final String KEYSPACE = "Keyspace1";
  public static final String CF1 = "Indexed1";
  public static final String CF2 = "Standard1";
  public static final ByteBuffer COLUMN = ByteBufferUtil.bytes("2012-01-01_12:12:12");
  public static final ByteBuffer VALUE = ByteBufferUtil.bytes("{\"observation\":\"8\"}");

  private static Logger logger = LoggerFactory.getLogger(TriggerTest.class);

  @org.junit.Before
  public void setUp() {
//    CassandraDaemon.main(null);
    CassandraDaemon cassandraService = new CassandraDaemon();
    cassandraService.activate();

    loadSchema(KEYSPACE, Arrays.asList(CF1, CF2));
  }

  @org.junit.Test
  public void testThrowingExceptionWhenInsertingColumn() throws Throwable {
    TTransport tr = new TFramedTransport(new TSocket(CASSANDRA_HOST, 9160));
    TProtocol proto = new TBinaryProtocol(tr);
    Cassandra.Client client = new Cassandra.Client(proto);
    tr.open();

    String rowkeyId = "PI123";
    long timestamp = System.currentTimeMillis();

    client.set_keyspace(KEYSPACE);
    ColumnParent parent = new ColumnParent(CF2);

    Column nameColumn = new Column(COLUMN);
    nameColumn.setValue(VALUE);
    nameColumn.setTimestamp(timestamp);

    try {
      client.insert(ByteBufferUtil.bytes(rowkeyId), parent, nameColumn, ConsistencyLevel.ONE);
      org.junit.Assert.fail("Should have thrown some Exception");
    }
    catch (org.apache.thrift.TApplicationException ex) {
      logger.warn("expected=" + ex.getClass());
      org.junit.Assert.assertEquals("Internal error processing insert", ex.getMessage());
    }

    tr.flush();
    tr.close();
  }

  private void loadSchema(String keyspaceName, List<String> colFamilyNames) {
    List<KSMetaData> schema = new ArrayList<KSMetaData>();

    Class<? extends AbstractReplicationStrategy> strategyClass = SimpleStrategy.class;
    Map<String, String> strategyOptions = KSMetaData.optsWithRF(1);

    CFMetaData[] cfDefs = new CFMetaData[colFamilyNames.size()];
    for (int i=0; i < colFamilyNames.size(); i++) {
      CFMetaData cfDef = new CFMetaData(KEYSPACE, colFamilyNames.get(i), ColumnFamilyType.Standard, UTF8Type.instance,
              null).keyValidator(UTF8Type.instance).defaultValidator(UTF8Type.instance);
      cfDefs[i] = cfDef;
    }

    KSMetaData validKsMetadata = KSMetaData.testMetadata(keyspaceName, strategyClass, strategyOptions, cfDefs);
    schema.add(validKsMetadata);

    Schema.instance.load(schema, Schema.instance.getVersion());
  }

}
