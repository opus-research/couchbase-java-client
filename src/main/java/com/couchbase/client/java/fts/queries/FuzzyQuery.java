/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
