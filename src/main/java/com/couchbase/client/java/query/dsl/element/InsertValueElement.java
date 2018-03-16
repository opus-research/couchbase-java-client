package com.couchbase.client.java.query.dsl.element;

import com.couchbase.client.java.query.dsl.Expression;

public class InsertValueElement implements Element {

  private final InsertPosition position;
  private final Expression id;
  private final Expression value;

  public InsertValueElement(InsertPosition position, Expression id, Expression value) {
    this.position = position;
    this.id = id;
    this.value = value;
  }

  @Override
  public String export() {
    return position.repr + "(" + id.toString() + ", " + value.toString() + ")";
  }

  public enum InsertPosition {
    INITIAL("VALUES "),
    NOT_INITIAL(", ");

    private final String repr;

    InsertPosition(String repr) {
      this.repr = repr;
    }
  }

}
