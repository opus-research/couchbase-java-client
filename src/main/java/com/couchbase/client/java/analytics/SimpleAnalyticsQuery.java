package com.couchbase.client.java.analytics;


import com.couchbase.client.java.document.json.JsonObject;

public class SimpleAnalyticsQuery extends AnalyticsQuery {

    private final String statement;
    private final AnalyticsParams params;

    SimpleAnalyticsQuery(String statement, AnalyticsParams params) {
        statement = statement.trim();
        if (!statement.endsWith(";")) {
            statement = statement + ";";
        }

        this.statement = statement;
        this.params = params;
    }

    public String statement() {
        return statement;
    }

    public AnalyticsParams params() {
        return params;
    }

    @Override
    public JsonObject query() {
        return JsonObject.create().put("statement", statement);
    }
}
