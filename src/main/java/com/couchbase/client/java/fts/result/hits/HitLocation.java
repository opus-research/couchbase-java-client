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
package com.couchbase.client.java.fts.result.hits;


class HitLocation {

    private final String field;
    private final String term;
    private final long pos;
    private final long start;
    private final long end;

    /**
     * can be null
     */
    private final long[] arrayPositions;

    public HitLocation(String field, String term, long pos, long start, long end, long[] arrayPositions) {
        this.field = field;
        this.term = term;
        this.pos = pos;
        this.start = start;
        this.end = end;
        this.arrayPositions = arrayPositions;
    }

    public HitLocation(String field, String term, long pos, long start, long end) {
        this(field, term, pos, start, end, null);
    }

    public String field() {
        return field;
    }

    public String term() {
        return term;
    }

    public long pos() {
        return pos;
    }

    public long start() {
        return start;
    }

    public long end() {
        return end;
    }

    /**
     * @return the array positions, or null if not applicable.
     */
    public long[] arrayPositions() {
        return arrayPositions;
    }
}
