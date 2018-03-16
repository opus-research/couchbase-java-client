/*
 * Copyright (C) 2015 Couchbase, Inc.
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

package com.couchbase.client.java.query;

import static com.couchbase.client.java.query.dsl.Expression.x;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.dsl.path.index.IndexType;
import com.couchbase.client.java.query.dsl.path.index.OnPath;
import com.couchbase.client.java.query.dsl.path.index.OnPrimaryPath;
import com.couchbase.client.java.query.dsl.path.index.UsingPath;
import com.couchbase.client.java.query.dsl.path.index.UsingWherePath;
import com.couchbase.client.java.query.dsl.path.index.WherePath;
import org.junit.Test;

public class IndexDslTest {

    @Test
    public void testCreateIndex() throws Exception {
        OnPath idx1 = Index.createIndex("test");
        assertFalse(idx1 instanceof Statement);

        UsingWherePath idx2 = idx1.on("beer-sample", x("abv"));
        assertTrue(idx2 instanceof Statement);

        Statement fullIndex = Index.createIndex("test")
                .on("beer-sample", x("abv"), x("ibu"))
                .using(IndexType.GSI)
                .where(x("abv").gt(10))
                .with(JsonObject.create().put("defer_build", true));

        assertEquals("CREATE INDEX `test` ON `beer-sample`(abv, ibu) " +
                "USING GSI WHERE abv > 10 WITH `{\"defer_build\":true}`", fullIndex.toString());
    }

    @Test
    public void testCreatePrimaryIndex() throws Exception {
        OnPrimaryPath idx1 = Index.createPrimaryIndex();
        assertFalse(idx1 instanceof Statement);

        UsingPath idx2 = idx1.on("beer-sample");
        assertTrue(idx2 instanceof Statement);

        Statement fullIndex = Index.createPrimaryIndex()
            .on("beer-sample")
            .using(IndexType.GSI)
            .with(JsonObject.create().put("defer_build", true));

        assertEquals("CREATE PRIMARY INDEX ON `beer-sample` " +
                "USING GSI WITH `{\"defer_build\":true}`", fullIndex.toString());
    }
}