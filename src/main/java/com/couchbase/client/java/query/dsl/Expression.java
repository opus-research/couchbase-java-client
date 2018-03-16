package com.couchbase.client.java.query.dsl;

/**
 * Represents a N1QL Expression.
 *
 * @author Michael Nitschinger
 * @since 2.0
 */
public class Expression {

    private final Object value;

    private Expression(Object value) {
        this.value = value;
    }

    public static Expression x(final String input) {
        return new Expression(input);
    }

    public static Expression s(final String input) {
        return new Expression("'" + input + "'");
    }

    public Expression and(Expression expression) {
        return fromInfix("AND", expression);
    }

    public Expression or(Expression expression) {
        return fromInfix("OR", expression);
    }

    public Expression not(Expression expression) {
        return fromInfix("NOT", expression);
    }

    public Expression eq(Expression expression) {
        return fromInfix("=", expression);
    }

    protected Expression fromInfix(String infix, Expression expression) {
        return new Expression(toString() + " " + infix + " " + expression);
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
