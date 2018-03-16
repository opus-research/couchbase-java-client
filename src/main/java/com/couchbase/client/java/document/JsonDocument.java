package com.couchbase.client.java.document;

public class JsonDocument extends AbstractDocument {

  public JsonDocument(String id, Object content) {
    super(id, content);
  }

  public JsonDocument(String id, Object content, int expiry) {
    super(id, content, expiry);
  }

  public JsonDocument(String id, Object content, long cas) {
    super(id, content, cas);
  }

  public JsonDocument(String id, Object content, long cas, int expiry) {
    super(id, content, cas, expiry);
  }

}
