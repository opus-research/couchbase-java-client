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

package com.couchbase.client.java.search.query;

import com.couchbase.client.java.document.json.JsonObject;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Sergey Avseyev
 */
public class DateRangeQuery extends SearchQuery {
    public static final double BOOST = 1.0;
    private static final boolean INCLUSIVE_START = true;
    private static final boolean INCLUSIVE_END = false;
    private static final String DATE_TIME_PARSER = "dateTimeOptional"; // also "goflexible"
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    private final Date start;
    private final Date end;
    private final boolean inclusiveStart;
    private final boolean inclusiveEnd;
    private final String field;
    private final String dateTimeParser;
    private final double boost;

    protected DateRangeQuery(Builder builder) {
        super(builder);
        start = builder.start;
        end = builder.end;
        inclusiveStart = builder.inclusiveStart;
        inclusiveEnd = builder.inclusiveEnd;
        field = builder.field;
        boost = builder.boost;
        dateTimeParser = builder.dateTimeParser;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Date start() {
        return start;
    }

    public Date end() {
        return end;
    }

    public boolean inclusiveStart() {
        return inclusiveStart;
    }

    public boolean inclusiveEnd() {
        return inclusiveEnd;
    }

    public String field() {
        return field;
    }

    public double boost() {
        return boost;
    }

    @Override
    public JsonObject queryJson() {
        return JsonObject.create()
                .put("start", DATE_FORMAT.format(start))
                .put("end", DATE_FORMAT.format(end))
                .put("inclusiveStart", inclusiveStart)
                .put("inclusiveEnd", inclusiveEnd)
                .put("field", field)
                .put("datetime_parser", dateTimeParser)
                .put("boost", boost);
    }

    public static class Builder extends SearchQuery.Builder {
        public double boost = BOOST;
        public String dateTimeParser = DATE_TIME_PARSER;
        private Date start;
        private Date end;
        private boolean inclusiveStart = INCLUSIVE_START;
        private boolean inclusiveEnd = INCLUSIVE_END;
        private String field;

        public DateRangeQuery build() {
            return new DateRangeQuery(this);
        }

        public Builder boost(double boost) {
            this.boost = boost;
            return this;
        }

        public Builder start(Date start) {
            this.start = start;
            return this;
        }

        public Builder end(Date end) {
            this.end = end;
            return this;
        }

        public Builder inclusiveStart(boolean inclusiveStart) {
            this.inclusiveStart = inclusiveStart;
            return this;
        }

        public Builder inclusiveEnd(boolean inclusiveEnd) {
            this.inclusiveEnd = inclusiveEnd;
            return this;
        }

        public Builder field(String field) {
            this.field = field;
            return this;
        }

        public Builder dateTimeParser(String dateTimeParser) {
            this.dateTimeParser = dateTimeParser;
            return this;
        }

    }
}