package com.couchbase.client.java.document;

/**
 * Common implementation of a {@link Document}.
 */
public abstract class AbstractDocument implements Document {

  private final String id;
  private final long cas;
  private final int expiry;
  private Object content;

  protected AbstractDocument(String id) {
    this(id, null, 0, 0);
  }

  protected AbstractDocument(String id, Object content) {
    this(id, content, 0, 0);
  }

  protected AbstractDocument(String id, Object content, int expiry) {
    this(id, content, 0, expiry);
  }

  protected AbstractDocument(String id, Object content, long cas) {
    this(id, content, cas, 0);
  }

  protected AbstractDocument(String id, Object content, long cas, int expiry) {
    this.id = id;
    this.cas = cas;
    this.expiry = expiry;
    this.content = content;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public long cas() {
    return cas;
  }

  @Override
  public int expiry() {
    return expiry;
  }

  @Override
  public Object content() {
    return content;
  }

  @Override
  public Document content(Object content) {
    this.content = content;
    return this;
  }
}
