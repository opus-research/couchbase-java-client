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

package com.couchbase.client.java.search.fulltext;

import com.couchbase.client.java.document.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Sergey Avseyev
 */
public class FullTextIndexMapping {
    private static final String TYPE_FIELD = "_type";
    private static final String DEFAULT_TYPE = "_default";
    private static final String DEFAULT_ANALYZER = "standard";
    private static final String DEFAULT_DATE_TIME_PARSER = "dateTimeOptional";
    private static final String DEFAULT_FIELD = "_all";
    private static final String BYTE_ARRAY_CONVERTER = "json";

    private final String typeField;
    private final String defaultType;
    private final String defaultAnalyzer;
    private final String defaultDateTimeParser;
    private final String defaultField;
    private final String byteArrayConverter;
    private final DocumentMapping defaultMapping;
    private final Map<String, DocumentMapping> types;

    protected FullTextIndexMapping(Builder builder) {
        typeField = builder.typeField;
        defaultType = builder.defaultType;
        defaultAnalyzer = builder.defaultAnalyzer;
        defaultDateTimeParser = builder.defaultDateTimeParser;
        defaultField = builder.defaultField;
        byteArrayConverter = builder.byteArrayConverter;
        defaultMapping = builder.defaultMapping;
        types = builder.types;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String typeField() {
        return typeField;
    }

    public String defaultType() {
        return defaultType;
    }

    public String defaultAnalyzer() {
        return defaultAnalyzer;
    }

    public String defaultDateTimeParser() {
        return defaultDateTimeParser;
    }

    public String defaultField() {
        return defaultField;
    }

    public String byteArrayConverter() {
        return byteArrayConverter;
    }

    public DocumentMapping defaultMapping() {
        return defaultMapping;
    }

    public Map<String, DocumentMapping> types() {
        return types;
    }

    public JsonObject json() {
        JsonObject json = JsonObject.create();
        json.put("typeField", typeField);
        json.put("defaultType", defaultType);
        json.put("defaultAnalyzer", defaultAnalyzer);
        json.put("defaultDateTimeParser", defaultDateTimeParser);
        json.put("defaultField", defaultField);
        json.put("byteArrayConverter", byteArrayConverter);
        json.put("defaultMapping", defaultMapping.json());
        if (types != null && !types.isEmpty()) {
            JsonObject typesJson = JsonObject.create();
            for (Map.Entry<String, DocumentMapping> entry : types.entrySet()) {
                typesJson.put(entry.getKey(), entry.getValue().json());
            }
            json.put("types", typesJson);
        }
        return json;
    }

    public static class Builder {
        public String typeField = TYPE_FIELD;
        public String defaultType = DEFAULT_TYPE;
        public String defaultAnalyzer = DEFAULT_ANALYZER;
        public String defaultDateTimeParser = DEFAULT_DATE_TIME_PARSER;
        public String defaultField = DEFAULT_FIELD;
        public String byteArrayConverter = BYTE_ARRAY_CONVERTER;
        public DocumentMapping defaultMapping = DocumentMapping.builder().build();
        public Map<String, DocumentMapping> types = new HashMap<String, DocumentMapping>();

        protected Builder() {
        }

        public FullTextIndexMapping build() {
            return new FullTextIndexMapping(this);
        }

        public Builder typeField(final String typeField) {
            this.typeField = typeField;
            return this;
        }

        public Builder defaultType(final String defaultType) {
            this.defaultType = defaultType;
            return this;
        }

        public Builder defaultAnalyzer(final String defaultAnalyzer) {
            this.defaultAnalyzer = defaultAnalyzer;
            return this;
        }

        public Builder defaultDateTimeParser(final String defaultDateTimeParser) {
            this.defaultDateTimeParser = defaultDateTimeParser;
            return this;
        }

        public Builder defaultField(final String defaultField) {
            this.defaultField = defaultField;
            return this;
        }

        public Builder byteArrayConverter(final String byteArrayConverter) {
            this.byteArrayConverter = byteArrayConverter;
            return this;
        }

        public Builder defaultMapping(final DocumentMapping defaultMapping) {
            this.defaultMapping = defaultMapping;
            return this;
        }

        public Builder types(final Map<String, DocumentMapping> types) {
            this.types = types;
            return this;
        }

        public Builder type(final String name, final DocumentMapping mapping) {
            types.put(name, mapping);
            return this;
        }
    }
}
