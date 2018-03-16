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

public class NumericRange {

    private final String name;
    private final Double min;
    private final Double max;

    public NumericRange(String name, Double min, Double max) {
        this.name = name;
        this.min = min;
        this.max = max;

        if (name == null) {
            throw new NullPointerException("Cannot create numeric range without a name");
        }
        if (min == null && max == null) {
            throw new NullPointerException("Cannot create numeric range without min nor max");
        }
    }

    public static NumericRange numericRange(String name, Double min, Double max) {
        return new NumericRange(name, min, max);
    }

    public String name() {
        return name;
    }

    public Double min() {
        return min;
    }

    public Double max() {
        return max;
    }
}
