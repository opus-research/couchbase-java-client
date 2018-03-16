package com.couchbase.client.java.repository.mapping;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Reflection based implementation for entity metadata.
 *
 * @author Michael Nitschinger
 * @since 2.2.0
 */
public class ReflectionBasedEntityMetadata implements EntityMetadata {

    private final List<PropertyMetadata> properties;
    private final PropertyMetadata idProperty;
    private final PropertyMetadata casProperty;

    public ReflectionBasedEntityMetadata(Class<?> sourceEntity) {
        properties = new ArrayList<PropertyMetadata>();

        PropertyMetadata idProperty = null;
        PropertyMetadata casProperty = null;
        for (Field field : sourceEntity.getDeclaredFields()) {
            PropertyMetadata property = new ReflectionBasedPropertyMetadata(field);
            properties.add(property);
            if (property.isId()) {
                idProperty = property;
            }
            if (property.isCAS()) {
                casProperty = property;
            }
        }

        this.idProperty = idProperty;
        this.casProperty = casProperty;
    }

    @Override
    public List<PropertyMetadata> properties() {
        return properties;
    }

    @Override
    public boolean hasIdProperty() {
        return idProperty != null;
    }

    @Override
    public PropertyMetadata idProperty() {
        return idProperty;
    }

    @Override
    public boolean hasCasProperty() {
        return casProperty != null;
    }

    @Override
    public PropertyMetadata casProperty() {
        return casProperty;
    }
}
