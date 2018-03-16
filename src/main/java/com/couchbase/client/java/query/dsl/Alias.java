package com.couchbase.client.java.query.dsl;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class Alias {

    private final String alias;
    private final Expression original;

    public Alias(String alias, Expression original) {
        this.alias = alias;
        this.original = original;
    }

    public static Alias alias(String alias, Expression original) {
        return new Alias(alias, original);
    }

    @Override
    public String toString() {
        return alias + " = " + original.toString();
    }
}
