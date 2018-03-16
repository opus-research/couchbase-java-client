package com.couchbase.client.java.query.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class CriteriaTest {

    @Test
    public void testAllPositiveCriteriaBuilder() {
        String expected = "a = \"foo\"" +
                " AND b > 1" +
                " AND c >= 2" +
                " AND d < 4" +
                " AND e <= 3" +
                " AND f LIKE \"%toto%\"" +
                " AND g IS NULL" +
                " AND h IS VALUED" +
                " AND i IS MISSING" +
                " AND \"bar\" IN j" +
                " AND k BETWEEN 100 AND 200";

        Criteria test = Criteria.of("a").equalTo("foo")
                .and("b").greaterThan(1)
                .and("c").greaterThanOrEqualTo(2)
                .and("d").lesserThan(4)
                .and("e").lesserThanOrEqualTo(3)
                .and("f").like("%toto%")
                .and("g").isNull()
                .and("h").isValued()
                .and("i").isMissing()
                .and("j").contains("bar")
                .and("k").between(100, 200);

        assertThat(test.toN1ql().toString()).isEqualTo(expected);
    }

    @Test
    public void testAllNegatedCriteriaBuilder() {
        String expected = "a != \"foo\"" +
                " AND b <= 1" +
                " AND c < 2" +
                " AND d >= 4" +
                " AND e > 3" +
                " AND f NOT LIKE \"%toto%\"" +
                " AND g IS NOT NULL" +
                " AND h IS NOT VALUED" +
                " AND i IS NOT MISSING" +
                " AND \"bar\" NOT IN j" +
                " AND k NOT BETWEEN 100 AND 200";

        Criteria test = Criteria.of("a").not().equalTo("foo")
                .and("b").not().greaterThan(1)
                .and("c").not().greaterThanOrEqualTo(2)
                .and("d").not().lesserThan(4)
                .and("e").not().lesserThanOrEqualTo(3)
                .and("f").not().like("%toto%")
                .and("g").not().isNull()
                .and("h").not().isValued()
                .and("i").not().isMissing()
                .and("j").not().contains("bar")
                .and("k").not().between(100, 200);

        assertThat(test.toN1ql().toString()).isEqualTo(expected);
    }
}