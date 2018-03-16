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
package com.couchbase.client.java.fts.queries;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.fts.SearchParams;
import com.couchbase.client.java.fts.SearchQuery;

public class FuzzyQuery extends SearchQuery {

    private final String term;
    private String field;
    private Integer prefixLength;
    private Integer fuzziness;

    public FuzzyQuery(String term, SearchParams searchParams) {
        super(searchParams);
        this.term = term;
    }

    @Override
    public FuzzyQuery boost(double boost) {
        super.boost(boost);
        return this;
    }

    public FuzzyQuery field(String field) {
        this.field = field;
        return this;
    }

    public FuzzyQuery prefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    public FuzzyQuery fuzziness(int fuzziness) {
        this.fuzziness = fuzziness;
        return this;
    }

    @Override
    protected void injectParams(JsonObject input) {
        super.injectParams(input);

        input.put("term", term);
        if (field != null) {
            input.put("field", field);
        }
        if (prefixLength != null) {
            input.put("prefix_length", prefixLength);
        }
        if (fuzziness != null) {
            input.put("fuzziness", fuzziness);
        }
    }
}
