package com.couchbase.client.java.query.dsl.element;

import com.couchbase.client.java.query.dsl.Expression;

public class UnsetElement implements Element {

  private final Expression path;
  private final Expression unsetFor;
  private final UnsetPosition insert;

  public UnsetElement(UnsetPosition insert, Expression path, Expression unsetFor) {
    this.path = path;
    this.unsetFor = unsetFor;
    this.insert = insert;
  }

  @Override
  public String export() {
    String uf = unsetFor == null ? "" : " " + unsetFor.toString();
    return insert.repr + path.toString() + uf;
  }

  public enum UnsetPosition {
    INITIAL("UNSET "),
    NOT_INITIAL(", ");

    private final String repr;

    UnsetPosition(String repr) {
      this.repr = repr;
    }
  }

}
