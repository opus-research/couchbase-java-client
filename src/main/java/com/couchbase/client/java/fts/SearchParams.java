/**
 * Copyright (C) 2016 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.java.fts;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.fts.facet.SearchFacet;

import java.util.concurrent.TimeUnit;

public class SearchParams {

    private Integer limit;
    private Integer skip;
    private Boolean explain;
    private HighlightStyle highlightStyle;
    private String[] highlightFields;
    private String[] fields;
    private SearchFacet[] facets;
    private Long serverSideTimeout;

    private SearchParams() {
    }

    public static SearchParams build() {
        return new SearchParams();
    }

    public SearchParams limit(int limit) {
        this.limit = limit;
        return this;
    }

    public SearchParams skip(int skip) {
        this.skip = skip;
        return this;
    }


    public SearchParams explain() {
        return explain(true);
    }

    public SearchParams explain(boolean explain) {
        this.explain = explain;
        return this;
    }

    public SearchParams highlight(HighlightStyle style, String... fields) {
        this.highlightStyle = style;
        if (fields != null && fields.length > 0) {
            highlightFields = fields;
        }
        return this;
    }

    public SearchParams fields(String... fields) {
        if (fields != null && fields.length > 0) {
            this.fields = fields;
        }
        return this;
    }

    public SearchParams facets(SearchFacet... facets) {
        if (facets != null && facets.length > 0) {
            this.facets = facets;
        }
        return this;
    }

    public SearchParams serverSideTimeout(long timeout, TimeUnit unit) {
        this.serverSideTimeout = unit.toMillis(timeout);
        return this;
    }

    public void injectParams(JsonObject queryJson) {
        if (limit != null && limit >= 0) {
            queryJson.put("size", limit);
        }
        if (skip != null && skip >= 0) {
            queryJson.put("from", skip);
        }
        if (explain != null) {
            queryJson.put("explain", explain);
        }
        if (highlightStyle != null) {
            JsonObject highlight = JsonObject.create();
            highlight.put("style", highlightStyle.name().toLowerCase());
            if (highlightFields != null && highlightFields.length > 0) {
                highlight.put("fields", JsonArray.from(highlightFields));
            }
            queryJson.put("highlight", highlight);
        }
        if (fields != null && fields.length > 0) {
            queryJson.put("fields", JsonArray.from(fields));
        }
        if (facets != null && facets.length > 0) {
            JsonObject facets = JsonObject.create();
            for (SearchFacet f : this.facets) {
                JsonObject facet = JsonObject.create();
                f.injectParams(facet);
                facets.put(f.name(), facet);
            }
            queryJson.put("facets", facets);
        }
        if(serverSideTimeout != null) {
            JsonObject control = JsonObject.empty();
            control.put("timeout", serverSideTimeout);
            queryJson.put("ctl", control);
        }
    }

}
