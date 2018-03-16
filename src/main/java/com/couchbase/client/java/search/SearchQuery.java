package com.couchbase.client.java.search;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * @author Sergey Avseyev
 */
public class SearchQuery {
    public static final int SIZE = 10;
    public static final int FROM = 0;
    public static final double BOOST = 1.0;
    private static final boolean EXPLAIN = false;
    private static final String HIGHLIGHT_STYLE = "html"; /* html, ansi */

    private final String query;
    private final double boost;
    private final int size;
    private final int from;
    private final String index;
    private final boolean explain;
    private final String highlightStyle;
    private final String[] highlightFields;
    private final String[] fields;

    protected SearchQuery(Builder builder) {
        query = builder.query;
        boost = builder.boost;
        size = builder.size;
        from = builder.from;
        index = builder.index;
        explain = builder.explain;
        highlightStyle = builder.highlightStyle;
        highlightFields = builder.highlightFields;
        fields = builder.fields;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String query() {
        return query;
    }

    public double boost() {
        return boost;
    }

    public int size() {
        return size;
    }

    public int from() {
        return from;
    }

    public String index() {
        return index;
    }

    public String[] fields() {
        return fields;
    }

    public JsonObject json() {
        JsonObject json = JsonObject.create();
        json.put("query", JsonObject.create()
                .put("query", query)
                .put("boost", boost));
        JsonObject highlightJson = JsonObject.create();
        if (highlightStyle != null) {
            highlightJson.put("style", highlightStyle);
        }
        if (highlightFields != null) {
            highlightJson.put("fields", JsonArray.from(highlightFields));
        }
        if (highlightJson.size() > 0) {
            json.put("highlight", highlightJson);
        }
        if (fields != null) {
            json.put("fields", JsonArray.from(fields));
        }
        json.put("size", size);
        json.put("from", from);
        json.put("explain", explain);
        return json;
    }

    public static class Builder {
        public double boost = BOOST;
        public boolean explain = EXPLAIN;
        public String highlightStyle = HIGHLIGHT_STYLE;
        public String[] highlightFields;
        private String query;
        private int size = SIZE;
        private int from = FROM;
        private String index;
        public String[] fields;

        protected Builder() {
        }

        public SearchQuery build() {
            return new SearchQuery(this);
        }

        public Builder index(String index) {
            this.index = index;
            return this;
        }

        public Builder boost(double boost) {
            this.boost = boost;
            return this;
        }

        public Builder query(String query) {
            this.query = query;
            return this;
        }

        public Builder size(int size) {
            this.size = size;
            return this;
        }

        public Builder from(int from) {
            this.from = from;
            return this;
        }

        public Builder explain(boolean explain) {
            this.explain = explain;
            return this;
        }

        public Builder highlightStyle(String highlightStyle) {
            this.highlightStyle = highlightStyle;
            return this;
        }

        public Builder highlightFields(String... highlightFields) {
            this.highlightFields = highlightFields;
            return this;
        }

        public Builder fields(String... fields) {
            this.fields = fields;
            return this;
        }
    }
}
