package com.couchbase.client.java.query.dsl;

import org.junit.Test;

import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;
import static org.junit.Assert.assertEquals;

public class ExpressionTest {

    @Test
    public void shouldUseRawExpression() {
        Expression result = x("count(*) > 5 != 'wtf'");
        assertEquals("count(*) > 5 != 'wtf'", result.toString());
    }
    @Test
    public void shouldWrapWithStringTicks() {
        Expression result = s("Hello World");
        assertEquals("'Hello World'", result.toString());
    }

    @Test
    public void shouldAndExpressions() {
        Expression result = x("rock").and(x("roll"));
        assertEquals("rock AND roll", result.toString());
    }

    @Test
    public void shouldOrExpressions() {
        Expression result = x("trick").or(x("treat"));
        assertEquals("trick OR treat", result.toString());
    }

}