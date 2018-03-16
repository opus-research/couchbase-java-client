package com.couchbase.client.java.view;

import com.couchbase.client.core.CouchbaseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a {@link DesignDocument} containing {@link View}s.
 *
 * @author Michael Nitschinger
 * @since 2.0
 */
public class DesignDocument {

    private static final String DEFAULT_LANGUAGE = "javascript";
    private static final ObjectMapper mapper = new ObjectMapper();

    private final String name;
    private final boolean development;
    private final String language;
    private final List<View> views;

    protected DesignDocument(final String name, final boolean development, final String language, final List<View> views) {
        this.name = name;
        this.development = development;
        this.language = language;
        this.views = views;
    }

    public static DesignDocument create(final String name, final List<View> views) {
        return new DesignDocument(name, false, DEFAULT_LANGUAGE, views);
    }

    public static DesignDocument create(final String name, final List<View> views, final boolean development) {
        return new DesignDocument(name, development, DEFAULT_LANGUAGE, views);
    }

    public static DesignDocument from(String name, boolean development, String raw) {
        try {
            Map<String, Object> parsed = mapper.readValue(raw, new TypeReference<HashMap<String, Object>>() {});
            String language = (String) parsed.get("language");
            List<View> views = new ArrayList<View>();

            if (parsed.containsKey("views")) {
                Map<String, Object> regulars = (Map<String, Object>) parsed.get("views");
                for (Map.Entry<String, Object> entry : regulars.entrySet()) {
                    String viewName = entry.getKey();
                    Map<String, String> functions = (Map<String, String>) entry.getValue();
                    views.add(DefaultView.create(viewName, functions.get("map"), functions.get("reduce")));
                }
            }
            if (parsed.containsKey("spatial")) {
                Map<String, String> spatials = (Map<String, String>) parsed.get("spatial");
                for (Map.Entry<String, String> entry : spatials.entrySet()) {
                    views.add(SpatialView.create(entry.getKey(), entry.getValue()));
                }
            }
            return new DesignDocument(name, development, language, views);
        } catch (IOException e) {
            throw new CouchbaseException("Could not parse raw design document JSON: " + raw);
        }
    }

    public boolean development() {
        return development;
    }

    public String name() {
        return name;
    }

    public String language() {
        return language;
    }

    public List<View> views() {
        return views;
    }

    @Override
    public String toString() {
        return "DesignDocument{" +
            "name='" + name + '\'' +
            ", development=" + development +
            ", language='" + language + '\'' +
            ", views=" + views +
            '}';
    }
}
