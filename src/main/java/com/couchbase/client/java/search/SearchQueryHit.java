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

package com.couchbase.client.java.search;

import com.couchbase.client.java.document.json.JsonArray;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Sergey Avseyev
 */
public class SearchQueryHit {
    private final String id;
    private final double score;
    private final String explanation;
    /**
     * field -> {term -> locations[]}
     */
    private final Map<String, Map<String, Location[]>> locations;
    /**
     * field -> fragments[]
     */
    private final Map<String, String[]> fragments;
    private final Map<String, Object> fields;

    public SearchQueryHit(String id, double score, String explanation,
                          Map<String, Map<String, Location[]>> locations,
                          Map<String, String[]> fragments,
                          Map<String, Object> fields) {
        this.id = id;
        this.score = score;
        this.explanation = explanation;
        this.locations = locations;
        this.fragments = fragments;
        this.fields = fields;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SearchQueryHit{id='" + id + "', score=" + score + ", fragments={");
        if (fragments != null) {
            List<String> entries = new ArrayList<String>(fragments.size());
            for (Map.Entry<String, String[]> fragment : fragments.entrySet()) {
                entries.add("\"" + fragment.getKey() + "\":" + JsonArray.from(fragment.getValue()).toString());
            }
            sb.append(String.join(", ", entries));
        }
        return sb.append("}}").toString();
    }

    public String id() {
        return id;
    }

    public double score() {
        return score;
    }

    public String explanation() {
        return explanation;
    }

    public Map<String, Map<String, Location[]>> locations() {
        return locations;
    }

    public Map<String, String[]> fragments() {
        return fragments;
    }

    public Map<String, Object> fields() {
        return fields;
    }

    public static class Location {
        // FIXME: why double?
        private final double pos;
        private final double start;
        private final double end;
        private final double[] arrayPositions;

        public Location(double pos, double start, double end, double[] arrayPositions) {
            this.pos = pos;
            this.start = start;
            this.end = end;
            this.arrayPositions = arrayPositions;
        }
    }
}
