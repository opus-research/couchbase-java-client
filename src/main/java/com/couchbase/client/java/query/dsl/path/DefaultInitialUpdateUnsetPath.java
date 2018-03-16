package com.couchbase.client.java.query.dsl.path;

import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.element.UnsetElement;

import static com.couchbase.client.java.query.dsl.Expression.x;

public class DefaultInitialUpdateUnsetPath extends DefaultMutateWherePath implements InitialUpdateUnsetPath {

  public DefaultInitialUpdateUnsetPath(AbstractPath parent) {
    super(parent);
  }

  @Override
  public UpdateUnsetPath unset(String path) {
    element(new UnsetElement(UnsetElement.UnsetPosition.INITIAL, x(path), null));
    return new DefaultUpdateUnsetPath(this);
  }

  @Override
  public UpdateUnsetPath unset(String path, Expression updateFor) {
    element(new UnsetElement(UnsetElement.UnsetPosition.INITIAL, x(path), updateFor));
    return new DefaultUpdateUnsetPath(this);
  }

  @Override
  public UpdateUnsetPath unset(Expression path) {
    element(new UnsetElement(UnsetElement.UnsetPosition.INITIAL, path, null));
    return new DefaultUpdateUnsetPath(this);
  }

  @Override
  public UpdateUnsetPath unset(Expression path, Expression updateFor) {
    element(new UnsetElement(UnsetElement.UnsetPosition.INITIAL, path, updateFor));
    return new DefaultUpdateUnsetPath(this);
  }
}
