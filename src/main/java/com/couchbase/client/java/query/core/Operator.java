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

/**
 * Short description of class
 *
 * @author Simon Baslé
 * @since X.X
 */
public enum Operator {
    EQUALS,
    GREATER_THAN, LESSER_THAN,
    GREATER_THAN_EQUALS, LESSER_THAN_EQUALS,
    LIKE, CONTAINS,
    IS_NULL, IS_MISSING, IS_VALUED,

    NOT_EQUALS, NOT_LIKE, NOT_CONTAINS,
    IS_NOT_NULL, IS_NOT_MISSING, BETWEEN, NOT_BETWEEN, IS_NOT_VALUED;
}
