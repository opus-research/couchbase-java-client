package com.couchbase.client.java.query.dsl.element;

import com.couchbase.client.java.query.dsl.Expression;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class KeysElement implements Element {

    private final Expression expression;

    private final ClauseType clauseType;

    public KeysElement(ClauseType clauseType, Expression expression) {
        this.clauseType = clauseType;
        this.expression = expression;
    }

    @Override
    public String export() {
        return clauseType.n1ql + expression.toString();
    }

    public static enum ClauseType {

        JOIN_ON("ON KEYS "),
        USE_KEYSPACE("USE KEYS "),
        JOIN_ON_PRIMARY("ON PRIMARY KEYS "),
        USE_KEYSPACE_PRIMARY("USE PRIMARY KEYS ");

        private final String n1ql;

        ClauseType(String n1ql) {
            this.n1ql = n1ql;
        }

    }
}
