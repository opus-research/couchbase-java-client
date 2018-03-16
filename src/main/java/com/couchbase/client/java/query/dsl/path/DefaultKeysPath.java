package com.couchbase.client.java.query.dsl.path;

import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.element.KeysElement;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class DefaultKeysPath extends DefaultLetPath implements KeysPath {

    public DefaultKeysPath(AbstractPath parent) {
        super(parent);
    }

    @Override
    public LetPath keys(Expression expression) {
        element(new KeysElement(expression));
        return new DefaultLetPath(this);
    }
}
