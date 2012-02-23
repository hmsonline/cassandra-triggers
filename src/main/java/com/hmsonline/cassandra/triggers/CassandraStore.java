package com.hmsonline.cassandra.triggers;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.KsDef;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraStore {
    private static Logger logger = LoggerFactory.getLogger(CassandraStore.class);
    private boolean initialized = false;
    private String keyspace = null;
    private String columnFamily = null;

    protected CassandraStore(String keyspace, String columnFamily) throws Exception {
      this.keyspace = keyspace;
      this.columnFamily = columnFamily;
      this.create(new String [] {});
    }
    
    protected CassandraStore(String keyspace, String columnFamily, String [] indexedColumns) throws Exception {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.create(indexedColumns);
    }

    Cassandra.Iface getConnection(String keyspace) throws Exception {
        CassandraServer server = new CassandraServer();
        if (keyspace != null) {
            server.set_keyspace(keyspace);
        }
        return server;
    }

    public synchronized void create(String [] indexedColumns) throws Exception {
        if (!initialized) {
            try {
                List<CfDef> cfDefList = new ArrayList<CfDef>();
                KsDef ksDef = new KsDef(this.getKeyspace(), "org.apache.cassandra.locator.SimpleStrategy", cfDefList);
                ksDef.putToStrategy_options("replication_factor", "1");
                getConnection(null).system_add_keyspace(ksDef);
            } catch (Exception e) {
                logger.debug("Did not create [" + this.getKeyspace() + ":" + this.getColumnFamily()
                        + "] (probably already there)");
            }
            try {
                CfDef columnFamily = new CfDef(this.getKeyspace(), this.getColumnFamily());
                columnFamily.setKey_validation_class("UTF8Type");
                columnFamily.setDefault_validation_class("UTF8Type");
                columnFamily.setComparator_type("UTF8Type");
                
                // add indexes on columns
                if (indexedColumns != null && indexedColumns.length > 0) {
                  for (Object indexedColumn : indexedColumns) {
                      if (indexedColumn != null) {
                          String indexedColumnStr = indexedColumn.toString();
                          if (StringUtils.isNotBlank(indexedColumnStr)) {
                              List<ColumnDef> columnMetadata = columnFamily.getColumn_metadata();
                              columnMetadata = columnMetadata != null ? columnMetadata : new ArrayList<ColumnDef>();
                              ColumnDef colDef = new ColumnDef();
                              colDef.setName(indexedColumnStr.getBytes());
                              colDef.index_type = IndexType.KEYS;
                              colDef.setIndex_name(keyspace + "_" + this.columnFamily + "_" + indexedColumnStr + "_INDEX");
                              columnMetadata.add(colDef);
                              columnFamily.setColumn_metadata(columnMetadata);
                          }
                      }
                  }
              }
                
                getConnection(this.getKeyspace()).system_add_column_family(columnFamily);
                initialized = true;
            } catch (Exception e) {
                logger.debug("Did not create [" + this.getKeyspace() + ":" + this.getColumnFamily()
                        + "] (probably already there)");
            }
        }
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

}
