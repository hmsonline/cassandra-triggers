package com.hmsonline.cassandra.triggers.dao;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.cassandra.triggers.PausedTrigger;
import com.hmsonline.cassandra.triggers.Trigger;

public class TriggerStore extends CassandraStore {
    private static Logger logger = LoggerFactory.getLogger(TriggerStore.class);

    public static final String KEYSPACE = "triggers";
    public static final String COLUMN_FAMILY = "Triggers";
    public static final String ENABLED = "enabled";
    public static final String PAUSED = "PAUSED";
    private static TriggerStore instance = null;
    private long lastFetchTime = -1;
    private static final int REFRESH_INTERVAL = 1000 * 30; // 30 seconds
    Map<String, List<Trigger>> triggerMap = null;
    Map<String, Trigger> triggerCache = null;


    public TriggerStore(String keyspace, String columnFamily) throws Exception {
        super(keyspace, columnFamily);
        logger.debug("Instantiated trigger store.");
    }

    public static synchronized TriggerStore getStore() throws Exception {
        if (instance == null)
            instance = new TriggerStore(KEYSPACE, COLUMN_FAMILY);
        return instance;
    }

    @SuppressWarnings("unchecked")
    public static Trigger getTrigger(String triggerClass, Map<String, Trigger> cache) throws Exception {
        try {
            if(cache.get(triggerClass) == null) {
                Class<Trigger> clazz = (Class<Trigger>) Class.forName(triggerClass);
                Trigger trigger = clazz.newInstance();
                cache.put(triggerClass, trigger);
            }
            return cache.get(triggerClass);
        } catch (Exception e) {
            logger.error("Could not create trigger class [" + triggerClass + "], it will NOT run.", e);
        }
        return null;
    }

    public synchronized Map<String, List<Trigger>> getTriggers() throws Exception {
        long currentTime = System.currentTimeMillis();
        long timeSinceRefresh = currentTime - this.lastFetchTime;
        if (timeSinceRefresh > REFRESH_INTERVAL) {
            this.lastFetchTime = currentTime;
            this.triggerMap = new HashMap<String, List<Trigger>>();
            this.triggerCache = new HashMap<String, Trigger>();
            SlicePredicate predicate = new SlicePredicate();
            SliceRange range = new SliceRange(ByteBufferUtil.bytes(""), ByteBufferUtil.bytes(""), false, 10);
            predicate.setSlice_range(range);

            KeyRange keyRange = new KeyRange(1000);
            keyRange.setStart_key(ByteBufferUtil.bytes(""));
            keyRange.setEnd_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            ColumnParent parent = new ColumnParent(COLUMN_FAMILY);
            List<KeySlice> rows = getConnection(KEYSPACE).get_range_slices(parent, predicate, keyRange,
                    READ_CONSISTENCY);
            for (KeySlice slice : rows) {
                String columnFamily = ByteBufferUtil.string(slice.key);
                triggerMap.put(columnFamily, processRow(slice, triggerCache));
            }
        }
        if(logger.isDebugEnabled()) {
            logger.debug("time in getTriggers: " + (System.currentTimeMillis() - currentTime));
        }
        return triggerMap;
    }

    public void insertTrigger(String keyspace, String columnFamily, String triggerName) throws InvalidRequestException,
            UnavailableException, TimedOutException, TException, Exception {
        List<Mutation> slice = new ArrayList<Mutation>();
        slice.add(getMutation(triggerName, ENABLED));
        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        Map<String, List<Mutation>> cfMutations = new HashMap<String, List<Mutation>>();
        cfMutations.put(COLUMN_FAMILY, slice);

        ByteBuffer rowKey = ByteBufferUtil.bytes(keyspace + ":" + columnFamily);
        mutationMap.put(rowKey, cfMutations);
        getConnection(KEYSPACE).batch_mutate(mutationMap, WRITE_CONSISTENCY);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private List<Trigger> processRow(KeySlice slice, Map<String, Trigger> cache) throws Exception {
        List<Trigger> triggers = new ArrayList<Trigger>();
        for (ColumnOrSuperColumn column : slice.columns) {
            String className = ByteBufferUtil.string(column.column.name);
            String enabled = ByteBufferUtil.string(column.column.value);
            if (PAUSED.equals(StringUtils.upperCase(className)) && ENABLED.equals(enabled)) {
                return new ArrayList(Arrays.asList(new Trigger[] { new PausedTrigger() }));
            } else if (enabled.equals(ENABLED)) {
                Trigger trigger = getTrigger(className, cache);
                if (trigger != null)
                    triggers.add(trigger);
            }
        }
        return triggers;
    }
}
