package com.couchbase.client.java.query.dsl.path;

import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.element.ReturningElement;

import static com.couchbase.client.java.query.dsl.Expression.x;

public class DefaultReturningPath extends AbstractPath implements ReturningPath {

  public DefaultReturningPath(AbstractPath parent) {
    super(parent);
  }

  @Override
  public Statement returning(String expression) {
    element(new ReturningElement(ReturningElement.ReturningType.REGULAR, x(expression)));
    return this;
  }

  @Override
  public Statement returning(Expression expression) {
    element(new ReturningElement(ReturningElement.ReturningType.REGULAR, expression));
    return this;
  }

  @Override
  public Statement returningRaw(String expression) {
    element(new ReturningElement(ReturningElement.ReturningType.RAW, x(expression)));
    return this;
  }

  @Override
  public Statement returningRaw(Expression expression) {
    element(new ReturningElement(ReturningElement.ReturningType.RAW, expression));
    return this;
  }

  @Override
  public Statement returningElement(String expression) {
    element(new ReturningElement(ReturningElement.ReturningType.ELEMENT, x(expression)));
    return this;
  }

  @Override
  public Statement returningElement(Expression expression) {
    element(new ReturningElement(ReturningElement.ReturningType.ELEMENT, expression));
    return this;
  }

}
