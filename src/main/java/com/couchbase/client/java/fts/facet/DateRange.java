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
package com.couchbase.client.java.fts.facet;

public class DateRange {

    private final String name;
    private final String start;
    private final String end;

    public DateRange(String name, String start, String end) {
        this.name = name;
        this.start = start;
        this.end = end;

        if (name == null) {
            throw new NullPointerException("Cannot create date range without a name");
        }
        if (start == null && end == null) {
            throw new NullPointerException("Cannot create date range without start nor end");
        }
    }

    public static DateRange dateRange(String name, String start, String end) {
        return new DateRange(name, start, end);
    }

    public String name() {
        return name;
    }

    public String start() {
        return start;
    }

    public String end() {
        return end;
    }
}
