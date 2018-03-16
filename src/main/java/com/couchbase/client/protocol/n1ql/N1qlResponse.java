package com.couchbase.client.protocol.n1ql;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class N1qlResponse {
    private int resultCount = 0;
    private List<JSONObject> results;

    protected N1qlResponse() {
        //pass
    }

    public int getResultCount() {
        return resultCount;
    }

    public List<JSONObject> getResults() {
        return results;
    }

    protected N1qlResponse parseJson(JSONObject json) throws JSONException {
        JSONArray jsonResults = json.getJSONArray("resultset");
        resultCount = jsonResults.length();
        results = new ArrayList<JSONObject>(resultCount);
        for(int i = 0; i < resultCount; ++i) {
            JSONObject jsonResult = jsonResults.getJSONObject(i);
            results.add(jsonResult);
        }
        return this;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
