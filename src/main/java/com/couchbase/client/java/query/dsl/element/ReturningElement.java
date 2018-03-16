package com.couchbase.client.java.query.dsl.element;

import com.couchbase.client.java.query.dsl.Expression;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class ReturningElement implements Element {

  private final ReturningType type;
  private final Expression exp;

  public ReturningElement(ReturningType type, Expression exp) {
    this.type = type;
    this.exp = exp;
  }

  @Override
  public String export() {
    return "RETURNING " + type.repr + exp.toString();
  }

  public enum ReturningType {
    REGULAR(""),
    RAW("RAW "),
    ELEMENT("ELEMENT ");

    private final String repr;

    ReturningType(String repr) {
      this.repr = repr;
    }
  }

}
