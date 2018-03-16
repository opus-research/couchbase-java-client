package com.couchbase.client.java.query.dsl.element;

import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class InsertSelectElement implements Element {

  private final Expression key;
  private final Expression value;
  private final Statement select;

  public InsertSelectElement(Expression key, Expression value, Statement select) {
    this.key = key;
    this.value = value;
    this.select = select;
  }

  @Override
  public String export() {
    String value = this.value == null ? "" : ", VALUE " + this.value.toString();
    return "(KEY "+ key + value + ") " + select.toString();
  }

}
