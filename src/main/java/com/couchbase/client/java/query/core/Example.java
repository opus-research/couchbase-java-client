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
package com.couchbase.client.java.query.core;

import static com.couchbase.client.java.query.core.Operator.EQUALS;

import java.util.Collections;
import java.util.Map;

import com.couchbase.client.java.document.json.JsonObject;

/**
 * Short description of class
 *
 * @author Simon Baslé
 * @since X.X
 */
public class Example extends CriteriaBase {


    protected Example(JsonObject example, Map<String, Operator> operators) {
        super();
        for (String key: example.getNames()) {
            Operator operator = operators.get(key);
            if (operator == null) {
                operator = EQUALS;
            }

            addCriteria(key, operator, example.get(key));
        }
    }

    public static Example of(JsonObject example) {
        return new Example(example, Collections.<String, Operator>emptyMap());
    }

    public static Example of(JsonObject example, Map<String, Operator> operators) {
        return new Example(example, operators);
    }
}
