package com.couchbase.client.java.query.dsl.element;

import com.couchbase.client.java.query.dsl.Expression;

public class SetElement implements Element {

  private final Expression path;
  private final Expression setFor;
  private final SetPosition insert;
  private final Expression value;

  public SetElement(SetPosition insert, Expression path, Expression value, Expression setFor) {
    this.path = path;
    this.setFor = setFor;
    this.insert = insert;
    this.value = value;
  }

  @Override
  public String export() {
    String uf = setFor == null ? "" : " " + setFor.toString();
    return insert.repr + path.toString() + " = " + value.toString() + uf;
  }

  public enum SetPosition {
    INITIAL("SET "),
    NOT_INITIAL(", ");

    private final String repr;

    SetPosition(String repr) {
      this.repr = repr;
    }
  }

}
