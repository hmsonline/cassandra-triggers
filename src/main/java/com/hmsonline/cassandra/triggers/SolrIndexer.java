package com.hmsonline.cassandra.triggers;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class SolrIndexer implements Indexer {

  public static String CONTENT_TYPE = "application/json";
  public static String XML_CONTENT_TYPE = "application/xml";
  public static String CHAR_SET = "UTF8";

  public static String DEFAULT_SOLR_URL = "http://localhost:8983/solr/";
  public static String DYNAMIC_FIELD_SUFFIX = "_t";

  private String solrUrl = null;

  public SolrIndexer() {
    this.solrUrl = DEFAULT_SOLR_URL;
  }

  public SolrIndexer(String solrUrl) {
    this.solrUrl = (solrUrl != null)? solrUrl : DEFAULT_SOLR_URL;
  }

  public SolrIndexer(String solrHost, String solrPort) {
    this.solrUrl = "http://" + solrHost + ":" + solrPort + "/solr/";
  }

  public void index(String columnFamily, String rowKey, String json) {
    JSONObject row = (JSONObject) JSONValue.parse(json);
    index(columnFamily, rowKey, row);
  }

  @SuppressWarnings("unchecked")
  public void index(String columnFamily, String rowKey, JSONObject row) {
    HttpClient client = new HttpClient();
    PostMethod post = new PostMethod(solrUrl + "update/json?commit=true");
    JSONObject document = new JSONObject();

    document.put("id", this.getDocumentId(columnFamily, rowKey));
    document.put("rowKey" + DYNAMIC_FIELD_SUFFIX, rowKey);
    document.put("columnFamily" + DYNAMIC_FIELD_SUFFIX, columnFamily);
    for (Object column : row.keySet()) {
      document.put(column.toString().toLowerCase() + DYNAMIC_FIELD_SUFFIX, row.get(column));
    }

    // Index using solr URL
    try {
      RequestEntity requestEntity = new StringRequestEntity("[" + document.toString() + "]", CONTENT_TYPE, CHAR_SET);
      post.setRequestEntity(requestEntity);
      client.executeMethod(post);
    }
    catch (Exception ex) {
      throw new RuntimeException("Could not send index update request", ex);
    }
    finally {
      post.releaseConnection();
    }
  }

  public void delete(String columnFamily, String rowKey) {
    HttpClient client = new HttpClient();
    PostMethod post = new PostMethod(solrUrl + "update?commit=true");
    String query = "id:" + this.getDocumentId(columnFamily, rowKey);

    // Commit using solr URL
    try {
    StringRequestEntity requestEntity = new StringRequestEntity("<delete><query>" + query + "</query></delete>",
            XML_CONTENT_TYPE, CHAR_SET);
    post.setRequestEntity(requestEntity);
    client.executeMethod(post);
    }
    catch (Exception ex) {
      throw new RuntimeException("Could not send index delete request", ex);
    }
    finally {
      post.releaseConnection();
    }
  }

  private String getDocumentId(String columnFamily, String rowKey) {
    return columnFamily + "." + rowKey;
  }

}