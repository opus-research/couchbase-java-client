package com.couchbase.client.java.analytics;

import com.couchbase.client.java.document.json.JsonObject;

import java.io.Serializable;

public abstract class AnalyticsQuery implements Serializable {

    private static final long serialVersionUID = 3758113456237959729L;

    public abstract AnalyticsParams params();

    public abstract JsonObject query();

    public static AnalyticsQuery simple(String statement) {
        return new SimpleAnalyticsQuery(statement, null);
    }

    public static AnalyticsQuery simple(String statement, AnalyticsParams params) {
        return new SimpleAnalyticsQuery(statement, params);
    }

}
