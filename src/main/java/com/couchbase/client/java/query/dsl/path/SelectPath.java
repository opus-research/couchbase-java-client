package com.couchbase.client.java.query.dsl.path;

import com.couchbase.client.java.query.dsl.Expression;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public interface SelectPath extends Path {

    FromPath select(Expression... expressions);

    FromPath selectAll(Expression... expressions);

    FromPath selectDistinct(Expression... expressions);

    FromPath selectRaw(Expression expression);

}
