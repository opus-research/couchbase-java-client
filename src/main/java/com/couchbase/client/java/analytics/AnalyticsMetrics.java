package com.couchbase.client.java.analytics;

import com.couchbase.client.java.document.json.JsonObject;


public class AnalyticsMetrics {

    private final JsonObject rawMetrics;

    public AnalyticsMetrics(JsonObject rawMetrics) {
        this.rawMetrics = rawMetrics;
    }

}
