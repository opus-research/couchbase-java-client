package com.couchbase.client.java.query.dsl.path;

import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;

public interface ReturningPath extends Statement, Path {

  Statement returning(String expression);
  Statement returning(Expression expression);

  Statement returningRaw(String expression);
  Statement returningRaw(Expression expression);

  Statement returningElement(String expression);
  Statement returningElement(Expression expression);

}