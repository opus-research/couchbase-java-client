/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.query.core;

import static com.couchbase.client.java.query.core.Operator.BETWEEN;
import static com.couchbase.client.java.query.core.Operator.CONTAINS;
import static com.couchbase.client.java.query.core.Operator.EQUALS;
import static com.couchbase.client.java.query.core.Operator.IS_MISSING;
import static com.couchbase.client.java.query.core.Operator.IS_NOT_MISSING;
import static com.couchbase.client.java.query.core.Operator.IS_NOT_NULL;
import static com.couchbase.client.java.query.core.Operator.IS_NOT_VALUED;
import static com.couchbase.client.java.query.core.Operator.IS_NULL;
import static com.couchbase.client.java.query.core.Operator.IS_VALUED;
import static com.couchbase.client.java.query.core.Operator.NOT_BETWEEN;
import static com.couchbase.client.java.query.core.Operator.NOT_CONTAINS;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;

import java.util.LinkedList;
import java.util.List;

import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple3;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.dsl.Expression;

/**
 * Short description of class
 *
 * @author Simon Baslé
 * @since X.X
 */
public abstract class CriteriaBase {

    private final List<Tuple3<String, Operator, Object>> criterias = new LinkedList<Tuple3<String, Operator, Object>>();

    protected void addCriteria(String field, Operator operator, Object value) {
        if (operator == null) {
            operator = EQUALS;
        }
        this.criterias.add(Tuple.create(field, operator, value));
    }

    public Expression toN1ql() {
        Expression where = null;

        for (Tuple3<String, Operator, Object> criteria : criterias) {
            String key = criteria.value1();
            Operator operator = criteria.value2();
            Object value = criteria.value3();

            Expression clause = x(key);
            //first deal with operators requiring 0 or 2 values and similar special cases
            if (operator == IS_MISSING) {
                clause = clause.isMissing();
            } else if (operator == IS_NULL) {
                clause = clause.isNull();
            } else if (operator == IS_VALUED) {
                clause = clause.isValued();
            } else if (operator == IS_NOT_MISSING) {
                clause = clause.isNotMissing();
            } else if (operator == IS_NOT_NULL) {
                clause = clause.isNotNull();
            } else if (operator == IS_NOT_VALUED) {
                clause = clause.isNotValued();
            } else if (operator == BETWEEN || operator == NOT_BETWEEN) {
                if (value instanceof JsonArray && ((JsonArray) value).size() == 2) {
                    JsonArray array = (JsonArray) value;
                    Object a = array.get(0);
                    Object b = array.get(1);

                    clause = operator == BETWEEN
                            ? clause.between(expressionFor(a)).and(expressionFor(b))
                            : clause.notBetween(expressionFor(a)).and(expressionFor(b));
                } else {
                    throw new IllegalArgumentException("the value for BETWEEN must be a JsonArray of size 2");
                }
            } else if (operator == CONTAINS) {
                clause = expressionFor(value).in(key);
            } else if (operator == NOT_CONTAINS) {
                clause = expressionFor(value).notIn(key);
            } else if (value == null) {
                //ignore
                continue;
            } else {
                Expression valueExpression = expressionFor(value);

                switch (operator) {
                    case EQUALS:
                        clause = clause.eq(valueExpression);
                        break;
                    case NOT_EQUALS:
                        clause = clause.ne(valueExpression);
                        break;
                    case LESSER_THAN:
                        clause = clause.lt(valueExpression);
                        break;
                    case LESSER_THAN_EQUALS:
                        clause = clause.lte(valueExpression);
                        break;
                    case GREATER_THAN:
                        clause = clause.gt(valueExpression);
                        break;
                    case GREATER_THAN_EQUALS:
                        clause = clause.gte(valueExpression);
                        break;
                    case LIKE:
                        clause = clause.like(s("" + value));
                        break;
                    case NOT_LIKE:
                        clause = clause.notLike(s("" + value));
                        break;
                    default:
                        throw new UnsupportedOperationException("Unknown operator " + operator);
                }
            }

            if (where == null) {
                where = clause;
            } else {
                where = where.and(clause);
            }
        }
        return where == null ? Expression.path() : where;
    }

    protected static Expression expressionFor(Object o) {
        if (o == null) return Expression.NULL();
        if (o instanceof Integer) return x((Integer) o);
        if (o instanceof Long) return x((Long) o);
        if (o instanceof Double) return x((Double) o);
        if (o instanceof Float) return x((Float) o);
        if (o instanceof Boolean) return x((Boolean) o);
        if (o instanceof JsonObject) return x((JsonObject) o);
        if (o instanceof JsonArray) return x((JsonArray) o);

        if (o instanceof String) return s((String) o);
        throw new IllegalArgumentException("Cannot convert type " + o.getClass().getName() + " to a N1QL Expression");
    }
}
