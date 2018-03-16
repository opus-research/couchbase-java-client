package com.couchbase.client.java.query.dsl;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class Sort {

    private final String expression;
    private final Order ordering;

    private Sort(final String expression, final Order ordering) {
        this.expression = expression;
        this.ordering = ordering;
    }

    /**
     * Use default sort, don't specify an order in the resulting expression.
     */
    public static Sort def(final String expression) {
        return new Sort(expression, null);
    }

    public static Sort asc(final String expression) {
        return new Sort(expression, Order.ASC);
    }

    public static Sort desc(final String expression) {
        return new Sort(expression, Order.DESC);
    }


    @Override
    public String toString() {
        if (ordering != null) {
            return expression + " " + ordering;
        } else {
            return expression;
        }
    }

    public static enum Order  {
        ASC,
        DESC
    }
}
