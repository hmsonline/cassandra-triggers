package com.hmsonline.cassandra.triggers;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

public class HectorMutationTest extends AbstractTriggerTest {
    private final static String CLUSTER_NAME = "TEST_CLUSTER";
    private final static String ROW_KEY = "TEST_ROW_KEY";
    
    @org.junit.Test
    public void testLogWrite() throws Throwable {
        Cluster cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, "localhost:9160");
        Map<String, String> columns = new HashMap<String,String>();
        columns.put("col1", "val1");
        columns.put("col2", "val2");
        this.persist(cluster, DATA_KEYSPACE, DATA_CF1, ROW_KEY, columns);
        List<LogEntry> logEntries = DistributedCommitLog.getLog().getPending();
        assertEquals(1, logEntries.size());
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
