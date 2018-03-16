package com.couchbase.client.java.query.dsl.path;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.dsl.Expression;

public interface InsertValuesPath extends ReturningPath {

  InsertValuesPath values(String id, Expression value);
  InsertValuesPath values(String id, JsonObject value);
  InsertValuesPath values(String id, JsonArray value);
  InsertValuesPath values(String id, String value);
  InsertValuesPath values(String id, int value);
  InsertValuesPath values(String id, long value);
  InsertValuesPath values(String id, double value);
  InsertValuesPath values(String id, float value);
  InsertValuesPath values(String id, boolean value);

  InsertValuesPath values(Expression id, Expression value);
  InsertValuesPath values(Expression id, JsonObject value);
  InsertValuesPath values(Expression id, JsonArray value);
  InsertValuesPath values(Expression id, String value);
  InsertValuesPath values(Expression id, int value);
  InsertValuesPath values(Expression id, long value);
  InsertValuesPath values(Expression id, double value);
  InsertValuesPath values(Expression id, float value);
  InsertValuesPath values(Expression id, boolean value);

}
