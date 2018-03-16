package com.couchbase.client.java.query.dsl;

import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.dsl.path.*;
import org.junit.Test;

import static com.couchbase.client.java.query.dsl.Expression.x;
import static org.junit.Assert.assertEquals;

/**
 * General tests of the query DSL.
 *
 * @author Michael Nitschinger
 */
public class SelectDslTest {

    //
    // ====================================
    // General Select-From Tests (select-from-clause)
    // ====================================
    //

    @Test
    public void testGroupBy() {
        Path path = new DefaultGroupByPath(null).groupBy(x("relation"));
        assertEquals("GROUP BY relation", path.toString());
    }

    @Test
    public void testGroupByWithHaving() {
        Path path = new DefaultGroupByPath(null).groupBy(x("relation")).having(x("count(*) > 1"));
        assertEquals("GROUP BY relation HAVING count(*) > 1", path.toString());
    }

    @Test
    public void testWhere() {
        Path path = new DefaultWherePath(null).where(x("age > 20"));
        assertEquals("WHERE age > 20", path.toString());
    }

    @Test
    public void testWhereWithGroupBy() {
        Path path = new DefaultWherePath(null).where(x("age > 20")).groupBy(x("age"));
        assertEquals("WHERE age > 20 GROUP BY age", path.toString());
    }

    @Test
    public void testWhereWithGroupByAndHaving() {
        Path path = new DefaultWherePath(null).where(x("age > 20")).groupBy(x("age")).having(x("count(*) > 10"));
        assertEquals("WHERE age > 20 GROUP BY age HAVING count(*) > 10", path.toString());
    }

    @Test
    public void test() {
        // TODO: implement letting for group by
        // TODO: implement let before where
    }

    //
    // ====================================
    // General Select Tests (select-clause)
    // ====================================
    //

    @Test
    public void testOrderBy() {
        Query query = new DefaultOrderByPath(null).orderBy(Sort.asc("firstname"));
        assertEquals("ORDER BY firstname ASC", query.toString());

        query = new DefaultOrderByPath(null).orderBy(Sort.asc("firstname"), Sort.desc("lastname"));
        assertEquals("ORDER BY firstname ASC, lastname DESC", query.toString());
    }

    @Test
    public void testOrderByWithLimit() {
        Query query = new DefaultOrderByPath(null).orderBy(Sort.asc("firstname")).limit(5);
        assertEquals("ORDER BY firstname ASC LIMIT 5", query.toString());
    }

    @Test
    public void testOrderByWithLimitAndOffset() {
        Query query = new DefaultOrderByPath(null)
            .orderBy(Sort.asc("firstname"), Sort.desc("lastname"))
            .limit(5)
            .offset(10);
        assertEquals("ORDER BY firstname ASC, lastname DESC LIMIT 5 OFFSET 10", query.toString());
    }

    @Test
    public void testOrderByWithOffset() {
        Query query = new DefaultOrderByPath(null)
            .orderBy(Sort.asc("firstname"), Sort.desc("lastname"))
            .offset(3);
        assertEquals("ORDER BY firstname ASC, lastname DESC OFFSET 3", query.toString());
    }

    @Test
    public void testOffset() {
        Query query = new DefaultOffsetPath(null).offset(3);
        assertEquals("OFFSET 3", query.toString());
    }

    @Test
    public void testLimitWithOffset() {
        Query query = new DefaultLimitPath(null).limit(4).offset(3);
        assertEquals("LIMIT 4 OFFSET 3", query.toString());
    }

}
