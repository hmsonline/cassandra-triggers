package com.hmsonline.cassandra.triggers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

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

import com.hmsonline.cassandra.triggers.dao.CommitLog;
import com.hmsonline.cassandra.triggers.dao.ConfigurationStore;
import com.hmsonline.cassandra.triggers.dao.ErrorLog;
import com.hmsonline.cassandra.triggers.dao.TriggerStore;

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
            started = true;
        }
        ErrorLog.getErrorLog().create(new String[] {});
        CommitLog.getCommitLog().create(new String[] {});
        TriggerStore.getStore().create(new String[] {});
        ConfigurationStore.getStore().create(new String[] {});
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

    public void persist(Cluster cluster, String keyspace, String columnFamily, String rowKey,
            Map<String, String> columns) throws Exception {
        ConfigurationStore.getStore().disableRefresh();
        ConfigurationStore.getStore().enableCommitLog();

        StringSerializer serializer = StringSerializer.get();
        Keyspace hectorKeyspace = HFactory.createKeyspace(keyspace, cluster);
        Mutator<String> mutator = HFactory.createMutator(hectorKeyspace, serializer);

        for (String columnName : columns.keySet()) {
            mutator.addInsertion(rowKey, columnFamily, HFactory.createStringColumn(columnName, columns.get(columnName)));
        }
        mutator.execute();
    }

}
