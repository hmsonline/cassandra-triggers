package com.hmsonline.cassandra.triggers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryPath;
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

public class IndexTriggerTest {

  public static final String CASSANDRA_HOST = "localhost";
  public static final String KEYSPACE = "Keyspace1";
  public static final String CF1 = "Indexed1";
  public static final String CF2 = "Standard1";
  public static final ByteBuffer COLUMN = ByteBufferUtil.bytes("2012-01-01_12:12:12");
  public static final ByteBuffer VALUE = ByteBufferUtil.bytes("{\"IndexTrigger\":\"42\"}");

  public static final String KS_TRIGGER = DistributedCommitLog.KEYSPACE;
  public static final String CF_TRIGGER = "Triggers";
  public static final String CF_CONFIG = "Configuration";
  public static final String CF_LOG = "CommitLog";

  private static Logger logger = LoggerFactory.getLogger(IndexTriggerTest.class);

  @org.junit.BeforeClass
  public static void setUp() throws Exception {
//    CassandraDaemon.main(null);
    CassandraDaemon cassandraService = new CassandraDaemon();
    cassandraService.activate();

    System.setProperty("solrHost", "http://localhost:8983/solr/");
    loadSchema();
    enableTriggers();
  }

  @org.junit.Test
  public void testIndexingLogEntry() {
    logger.debug("111 mock data for indexing");

    LogEntry logEntry = new LogEntry();
    logEntry.setColumnFamily(CF2);
    logEntry.setKeyspace(KEYSPACE);
    logEntry.setRowKey(ByteBufferUtil.bytes("PI123"));
    logEntry.setConsistencyLevel(ConsistencyLevel.ONE);
    logEntry.setUuid("PI123");
    logEntry.setStatus(LogEntryStatus.COMMITTED);

    IndexTrigger trigger = new IndexTrigger();
    trigger.process(logEntry);
  }

  @org.junit.Test
  public void testInsertingColumn() throws Throwable {
    TTransport tr = new TFramedTransport(new TSocket(CASSANDRA_HOST, 9160));
    TProtocol proto = new TBinaryProtocol(tr);
    Cassandra.Client client = new Cassandra.Client(proto);
    tr.open();

    String rowkeyId = "PI123";
    long timestamp = System.currentTimeMillis();

    client.set_keyspace(KEYSPACE);
    ColumnParent parent = new ColumnParent(CF2);

    Column nameColumn = new Column().setName(COLUMN);
    nameColumn.setValue(VALUE);
    nameColumn.setTimestamp(timestamp);

    client.insert(ByteBufferUtil.bytes(rowkeyId), parent, nameColumn, ConsistencyLevel.ONE);

    tr.close();
  }

  //
  // PRIVATE METHODS
  //
  private static void loadSchema() {
    List<KSMetaData> schema = new ArrayList<KSMetaData>();

    Class<? extends AbstractReplicationStrategy> strategyClass = SimpleStrategy.class;
    Map<String, String> strategyOptions = KSMetaData.optsWithRF(1);

    CFMetaData cf1 = new CFMetaData(KEYSPACE, CF1, ColumnFamilyType.Standard, UTF8Type.instance,
            null).keyValidator(UTF8Type.instance).defaultValidator(UTF8Type.instance);
    CFMetaData cf2 = new CFMetaData(KEYSPACE, CF2, ColumnFamilyType.Standard, UTF8Type.instance,
            null).keyValidator(UTF8Type.instance).defaultValidator(UTF8Type.instance);

    KSMetaData validKsMetadata = KSMetaData.testMetadata(KEYSPACE, strategyClass, strategyOptions, cf1, cf2);
    schema.add(validKsMetadata);

    CFMetaData cf3 = new CFMetaData(KS_TRIGGER, CF_TRIGGER, ColumnFamilyType.Standard, UTF8Type.instance,
            null).keyValidator(UTF8Type.instance).defaultValidator(UTF8Type.instance);
    CFMetaData cf4 = new CFMetaData(KS_TRIGGER, CF_CONFIG, ColumnFamilyType.Standard, UTF8Type.instance,
            null).keyValidator(UTF8Type.instance).defaultValidator(UTF8Type.instance);
    CFMetaData cf5 = new CFMetaData(KS_TRIGGER, CF_LOG, ColumnFamilyType.Standard, UTF8Type.instance,
            null).keyValidator(UTF8Type.instance).defaultValidator(UTF8Type.instance);

    KSMetaData validTriggerKsMetadata = KSMetaData.testMetadata(KS_TRIGGER, strategyClass, strategyOptions, cf3, cf4, cf5);
    schema.add(validTriggerKsMetadata);

    Schema.instance.load(schema, Schema.instance.getVersion());
  }

  private static void enableTriggers() throws Exception {
    Table table = Table.open(KS_TRIGGER);
    ColumnFamilyStore cfs = table.getColumnFamilyStore(CF_CONFIG);

    RowMutation rm = new RowMutation(table.name, ByteBufferUtil.bytes("CommitLog"));
    rm.add(new QueryPath(cfs.getColumnFamilyName(), null, ByteBufferUtil.bytes("enabled")),
            ByteBufferUtil.bytes("true"), System.currentTimeMillis());

    rm.apply();
    cfs.forceBlockingFlush();
  }

}
