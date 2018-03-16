package com.couchbase.client.java.query.dsl.path;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.element.InsertSelectElement;
import com.couchbase.client.java.query.dsl.element.InsertValueElement;

import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;

public class DefaultInitialInsertPath extends DefaultReturningPath implements InitialInsertPath {

  private static final InsertValueElement.InsertPosition POS = InsertValueElement.InsertPosition.INITIAL;

  public DefaultInitialInsertPath(AbstractPath parent) {
    super(parent);
  }

  @Override
  public InsertValuesPath values(String id, JsonObject value) {
    element(new InsertValueElement(POS, s(id), x(value)));
    return new DefaultInsertValuesPath(this);
  }

  @Override
  public InsertValuesPath values(String id, Expression value) {
    element(new InsertValueElement(POS, s(id), value));
    return new DefaultInsertValuesPath(this);
  }

  @Override
  public InsertValuesPath values(String id, JsonArray value) {
    element(new InsertValueElement(POS, s(id), x(value)));
    return new DefaultInsertValuesPath(this);
  }

  @Override
  public InsertValuesPath values(String id, String value) {
    element(new InsertValueElement(POS, s(id), x(value)));
    return new DefaultInsertValuesPath(this);
  }

  @Override
  public InsertValuesPath values(String id, int value) {
    element(new InsertValueElement(POS, s(id), x(value)));
    return new DefaultInsertValuesPath(this);
  }

  @Override
  public InsertValuesPath values(String id, long value) {
    element(new InsertValueElement(POS, s(id), x(value)));
    return new DefaultInsertValuesPath(this);  }

  @Override
  public InsertValuesPath values(String id, double value) {
    element(new InsertValueElement(POS, s(id), x(value)));
    return new DefaultInsertValuesPath(this);  }

  @Override
  public InsertValuesPath values(String id, float value) {
    element(new InsertValueElement(POS, s(id), x(value)));
    return new DefaultInsertValuesPath(this);  }

  @Override
  public InsertValuesPath values(String id, boolean value) {
    element(new InsertValueElement(POS, s(id), x(value)));
    return new DefaultInsertValuesPath(this);
  }

  @Override
  public InsertValuesPath values(Expression id, Expression value) {
    element(new InsertValueElement(POS, id, value));
    return new DefaultInsertValuesPath(this);
  }

  @Override
  public InsertValuesPath values(Expression id, JsonObject value) {
    element(new InsertValueElement(POS, id, x(value)));
    return new DefaultInsertValuesPath(this);
  }

  @Override
  public InsertValuesPath values(Expression id, JsonArray value) {
    element(new InsertValueElement(POS, id, x(value)));
    return new DefaultInsertValuesPath(this);
  }

  @Override
  public InsertValuesPath values(Expression id, String value) {
    element(new InsertValueElement(POS, id, x(value)));
    return new DefaultInsertValuesPath(this);
  }

  @Override
  public InsertValuesPath values(Expression id, int value) {
    element(new InsertValueElement(POS, id, x(value)));
    return new DefaultInsertValuesPath(this);
  }

  @Override
  public InsertValuesPath values(Expression id, long value) {
    element(new InsertValueElement(POS, id, x(value)));
    return new DefaultInsertValuesPath(this);
  }

  @Override
  public InsertValuesPath values(Expression id, double value) {
    element(new InsertValueElement(POS, id, x(value)));
    return new DefaultInsertValuesPath(this);
  }

  @Override
  public InsertValuesPath values(Expression id, float value) {
    element(new InsertValueElement(POS, id, x(value)));
    return new DefaultInsertValuesPath(this);
  }

  @Override
  public InsertValuesPath values(Expression id, boolean value) {
    element(new InsertValueElement(POS, id, x(value)));
    return new DefaultInsertValuesPath(this);
  }

  @Override
  public ReturningPath select(Expression key, Statement select) {
    element(new InsertSelectElement(key, null, select));
    return new DefaultReturningPath(this);
  }

  @Override
  public ReturningPath select(Expression key, Expression value, Statement select) {
    element(new InsertSelectElement(key, value, select));
    return new DefaultReturningPath(this);
  }

  @Override
  public ReturningPath select(Expression key, String value, Statement select) {
    element(new InsertSelectElement(key, x(value), select));
    return new DefaultReturningPath(this);
  }

  @Override
  public ReturningPath select(String key, Statement select) {
    element(new InsertSelectElement(x(key), null, select));
    return new DefaultReturningPath(this);
  }

  @Override
  public ReturningPath select(String key, String value, Statement select) {
    element(new InsertSelectElement(x(key), x(value), select));
    return new DefaultReturningPath(this);
  }

  @Override
  public ReturningPath select(String key, Expression value, Statement select) {
    element(new InsertSelectElement(x(key), value, select));
    return new DefaultReturningPath(this);
  }
}
