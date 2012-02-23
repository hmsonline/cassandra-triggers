package com.hmsonline.cassandra.triggers;

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
import org.apache.cassandra.thrift.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTriggerTest {
    public static final String DATA_KEYSPACE = "Keyspace1";
    public static final String DATA_CF1 = "Indexed1";
    public static final String DATA_CF2 = "Standard1";
    private static Logger logger = LoggerFactory.getLogger(AbstractTriggerTest.class);
    private static boolean started = false;

    @org.junit.Before
    public void setUp() throws Exception {
        if (!started) {
            CassandraDaemon cassandraService = new CassandraDaemon();
            cassandraService.activate();
            try {
                loadDataSchema(DATA_KEYSPACE, Arrays.asList(DATA_CF1, DATA_CF2));
            } catch (Throwable t) {
                logger.debug("Received error when bootstrapping data schema, most likely it exists already."
                        + t.getMessage());
            }
            // loadTriggerSchema();
            started = true;
        }
    }

    private void loadDataSchema(String keyspaceName, List<String> colFamilyNames) {
        List<KSMetaData> schema = new ArrayList<KSMetaData>();
        Class<? extends AbstractReplicationStrategy> strategyClass = SimpleStrategy.class;
        Map<String, String> strategyOptions = KSMetaData.optsWithRF(1);

        CFMetaData[] cfDefs = new CFMetaData[colFamilyNames.size()];
        for (int i = 0; i < colFamilyNames.size(); i++) {
            CFMetaData cfDef = new CFMetaData(keyspaceName, colFamilyNames.get(i), ColumnFamilyType.Standard,
                    UTF8Type.instance, null);
            cfDefs[i] = cfDef;
        }

        KSMetaData validKsMetadata = KSMetaData.testMetadata(keyspaceName, strategyClass, strategyOptions, cfDefs);
        schema.add(validKsMetadata);

        Schema.instance.load(schema, Schema.instance.getVersion());
        logger.debug("======================= LOADED DATA SCHEMA FOR TESTS ==========================");
    }

    private void loadTriggerSchema() {
        List<KSMetaData> schema = new ArrayList<KSMetaData>();
        Class<? extends AbstractReplicationStrategy> strategyClass = SimpleStrategy.class;
        Map<String, String> strategyOptions = KSMetaData.optsWithRF(1);

        CFMetaData[] cfDefs = new CFMetaData[3];
        cfDefs[0] = new CFMetaData(TriggerStore.KEYSPACE, TriggerStore.COLUMN_FAMILY, ColumnFamilyType.Standard,
                UTF8Type.instance, null);
        cfDefs[1] = new CFMetaData(ConfigurationStore.KEYSPACE, ConfigurationStore.COLUMN_FAMILY,
                ColumnFamilyType.Standard, UTF8Type.instance, null);
        cfDefs[2] = new CFMetaData(DistributedCommitLog.KEYSPACE, DistributedCommitLog.COLUMN_FAMILY,
                ColumnFamilyType.Standard, UTF8Type.instance, null);

        KSMetaData validKsMetadata = KSMetaData.testMetadata(DistributedCommitLog.KEYSPACE, strategyClass,
                strategyOptions, cfDefs);
        schema.add(validKsMetadata);

        Schema.instance.load(schema, Schema.instance.getVersion());
        logger.debug("======================= LOADED TRIGGER SCHEMA FOR TESTS ==========================");
    }

}
