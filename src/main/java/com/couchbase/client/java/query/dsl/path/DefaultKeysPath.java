package com.couchbase.client.java.query.dsl.path;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.element.KeysElement;

import static com.couchbase.client.java.query.dsl.Expression.x;

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
        //TODO use primary variation?
        element(new KeysElement(KeysElement.ClauseType.JOIN_ON, expression));
        return new DefaultLetPath(this);
    }

    @Override
    public LetPath keys(String... keys) {
        return keys(JsonArray.from(keys));
    }

    @Override
    public LetPath keys(JsonArray keys) {
        return keys(x(keys));
    }

    @Override
    public LetPath useKeys(Expression expression) {
        //TODO use primary variation?
        element(new KeysElement(KeysElement.ClauseType.USE_KEYSPACE, expression));
        return new DefaultLetPath(this);
    }

    @Override
    public LetPath useKeys(String... keys) {
        if (keys.length == 1) {
            return useKeys(Expression.s(keys[0]));
        }
        return useKeys(JsonArray.from(keys));
    }

    @Override
    public LetPath useKeys(JsonArray keys) {
        return useKeys(x(keys));
    }
}
