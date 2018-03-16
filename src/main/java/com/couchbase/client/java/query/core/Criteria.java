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

import static com.couchbase.client.java.query.core.Operator.*;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;

import java.util.HashMap;
import java.util.Map;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.dsl.Expression;

/**
 * Short description of class
 *
 * @author Simon Baslé
 * @since X.X
 */
public class Criteria extends CriteriaBase {

    protected Criteria() {
        super();
    }

    public CriteriaBuilder and(String field) {
        return new CriteriaBuilder(field);
    }

    public static CriteriaBuilder of(String field) {
        return new Criteria().and(field);
    }

    public final class CriteriaBuilder {
        private final String field;
        private boolean negate;

        public CriteriaBuilder(String field) {
            this.field = field;
            this.negate = false;
        }

        public CriteriaBuilder not() {
            this.negate = !this.negate;
            return this;
        }

        public Criteria equalTo(Object value) {
            return updateExample(EQUALS, value);
        }

        public Criteria lesserThan(Object value) {
            return updateExample(LESSER_THAN, value);
        }

        public Criteria lesserThanOrEqualTo(Object value) {
            return updateExample(LESSER_THAN_EQUALS, value);
        }

        public Criteria greaterThan(Object value) {
            return updateExample(GREATER_THAN, value);
        }

        public Criteria greaterThanOrEqualTo(Object value) {
            return updateExample(GREATER_THAN_EQUALS, value);
        }

        public Criteria between(Object a, Object b) {
            return updateExample(BETWEEN, JsonArray.from(a, b));
        }

        public Criteria like(String value) {
            return updateExample(LIKE, value);
        }

        public Criteria contains(Object value) {
            return updateExample(CONTAINS, value);
        }

        public Criteria isNull() {
            return updateExample(IS_NULL, null);
        }

        public Criteria isMissing() {
            return updateExample(IS_MISSING, null);
        }

        public Criteria isValued() {
            return updateExample(IS_VALUED, null);
        }

        private Criteria updateExample(Operator operator, Object value) {
            if (this.negate) {
                operator = doNegate(operator);
            }
            Criteria.this.addCriteria(field, operator, value);
            return Criteria.this;
        }

        private Operator doNegate(Operator operatorToNegateOrNull) {
            if (operatorToNegateOrNull == null)
                return NOT_EQUALS;
            switch (operatorToNegateOrNull) {
                case EQUALS:
                    return NOT_EQUALS;
                case GREATER_THAN:
                    return LESSER_THAN_EQUALS;
                case GREATER_THAN_EQUALS:
                    return LESSER_THAN;
                case LESSER_THAN:
                    return GREATER_THAN_EQUALS;
                case LESSER_THAN_EQUALS:
                    return GREATER_THAN;
                case LIKE:
                    return NOT_LIKE;
                case CONTAINS:
                    return NOT_CONTAINS;
                case IS_NULL:
                    return IS_NOT_NULL;
                case IS_MISSING:
                    return IS_NOT_MISSING;
                case IS_VALUED:
                    return IS_NOT_VALUED;
                case BETWEEN:
                    return NOT_BETWEEN;
                default:
                    return operatorToNegateOrNull;
            }
        }
    }
}
