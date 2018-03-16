/**
 * Copyright (C) 2015 Couchbase, Inc.
 * <p/>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p/>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p/>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.client.java.search.bleve;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

import java.util.List;
import java.util.Map;

/**
 * @author Sergey Avseyev
 */
public class DocumentMapping {
    private static final boolean ENABLED = true;
    private static final boolean DYNAMIC = true;
    private static final String DEFAULT_ANALYZER = "";

    private final boolean enabled;
    private final boolean dynamic;
    private final String defaultAnalyzer;
    private final Map<String, DocumentMapping> properties;
    private final List<FieldMapping> fields;

    protected DocumentMapping(Builder builder) {
        enabled = builder.enabled;
        dynamic = builder.dynamic;
        defaultAnalyzer = builder.defaultAnalyzer;
        properties = builder.properties;
        fields = builder.fields;
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean enabled() {
        return enabled;
    }

    public boolean dynamic() {
        return dynamic;
    }

    public String defaultAnalyzer() {
        return defaultAnalyzer;
    }

    public Map<String, DocumentMapping> properties() {
        return properties;
    }

    public List<FieldMapping> fields() {
        return fields;
    }

    public JsonObject json() {
        JsonObject json = JsonObject.create();
        json.put("enabled", enabled);
        json.put("dynamic", dynamic);
        json.put("defaultAnalyzer", defaultAnalyzer);
        if (properties == null) {
            json.putNull("properties");
        } else {
            JsonObject propertiesJson = JsonObject.create();
            for (Map.Entry<String, DocumentMapping> entry : properties.entrySet()) {
                propertiesJson.put(entry.getKey(), entry.getValue().json());
            }
            json.put("properties", propertiesJson);
        }
        if (fields == null) {
            json.putNull("fields");
        } else {
            JsonArray fieldsJson = JsonArray.create();
            for (FieldMapping field : fields) {
                fieldsJson.add(field.json());
            }
            json.put("fields", fieldsJson);
        }
        return json;
    }

    public static class Builder {

        public boolean enabled = ENABLED;
        public boolean dynamic = DYNAMIC;
        public String defaultAnalyzer = DEFAULT_ANALYZER;
        public Map<String, DocumentMapping> properties = null;
        public List<FieldMapping> fields = null;

        protected Builder() {
        }

        public DocumentMapping build() {
            return new DocumentMapping(this);
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder dynamic(boolean dynamic) {
            this.dynamic = dynamic;
            return this;
        }

        public Builder defaultAnalyzer(String defaultAnalyzer) {
            this.defaultAnalyzer = defaultAnalyzer;
            return this;
        }

        public Builder properties(Map<String, DocumentMapping> properties) {
            this.properties = properties;
            return this;
        }

        public Builder fields(List<FieldMapping> fields) {
            this.fields = fields;
            return this;
        }
    }

}