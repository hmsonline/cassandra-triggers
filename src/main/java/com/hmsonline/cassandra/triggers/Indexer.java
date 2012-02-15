package com.hmsonline.cassandra.triggers;

import org.json.simple.JSONObject;

public interface Indexer {

  /**
   * Indexes the content passed into the method,<br>
   * Assumes single-level/flat structure of the JSON.
   * 
   * @param columnFamily
   * @param rowKey
   * @param json
   */
  public void index(String columnFamily, String rowKey, String json);

  /**
   * Indexes the JSONObject content passed into the method.
   * 
   * @param columnFamily
   * @param rowKey
   * @param json
   */
  public void index(String columnFamily, String rowKey, JSONObject json);

  /**
   * Removes a row from the index.
   */
  public void delete(String columnFamily, String rowKey);
}