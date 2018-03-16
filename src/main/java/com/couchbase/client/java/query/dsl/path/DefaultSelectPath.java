package com.couchbase.client.java.query.dsl.path;


import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.element.SelectElement;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class DefaultSelectPath extends AbstractPath implements SelectPath {

    public DefaultSelectPath(AbstractPath parent) {
        super(parent);
    }

    @Override
    public FromPath select(Expression... expressions) {
        element(new SelectElement(SelectType.DEFAULT, expressions));
        return new DefaultFromPath(this);
    }

    @Override
    public FromPath selectAll(Expression... expressions) {
        element(new SelectElement(SelectType.ALL, expressions));
        return new DefaultFromPath(this);
    }

    @Override
    public FromPath selectDistinct(Expression... expressions) {
        element(new SelectElement(SelectType.DISTINCT, expressions));
        return new DefaultFromPath(this);
    }

    @Override
    public FromPath selectRaw(Expression expression) {
        element(new SelectElement(SelectType.RAW, expression));
        return new DefaultFromPath(this);
    }
}
