package com.couchbase.client.java.query.core;

import static com.couchbase.client.java.query.core.Operator.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import org.junit.Test;

public class ExampleTest {

    @Test
    public void testOf() throws Exception {
        String expected = "lastName = \"Doe\" AND age = 21";

        JsonObject example = JsonObject.create().put("lastName", "Doe").put("age", 21);
        Example test = Example.of(example);

        assertThat(test.toN1ql().toString()).isEqualTo(expected);
    }

    @Test
    public void testOperatorIsMissingIgnoresValue() {
        String expected = "lastName IS MISSING AND age = 21";

        JsonObject example = JsonObject.create().put("lastName", "Doe").put("age", 21);
        Example test = Example.of(example, Collections.singletonMap("lastName", IS_MISSING));

        assertThat(test.toN1ql().toString()).isEqualTo(expected);
    }

    @Test
    public void testOperatorIsNullIgnoresValue() {
        String expected = "lastName IS NULL AND age = 21";

        JsonObject example = JsonObject.create().put("lastName", "Doe").put("age", 21);
        Example test = Example.of(example, Collections.singletonMap("lastName", IS_NULL));

        assertThat(test.toN1ql().toString()).isEqualTo(expected);
    }

    @Test
    public void testOperatorIsValuedIgnoresValue() {
        String expected = "lastName IS VALUED AND age = 21";

        JsonObject example = JsonObject.create().put("lastName", "Doe").put("age", 21);
        Example test = Example.of(example, Collections.singletonMap("lastName", IS_VALUED));

        assertThat(test.toN1ql().toString()).isEqualTo(expected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBetweenRequiresJsonArray() {
        Example test = Example.of(JsonObject.create().put("date", 1000),
                Collections.singletonMap("date", BETWEEN));

        test.toN1ql();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBetweenRequiresJsonArraySize2() {
        Example test = Example.of(JsonObject.create().put("date", JsonArray.from(1000, 2000, 3000)),
                Collections.singletonMap("date", BETWEEN));

        test.toN1ql();
    }

    @Test
    public void testBetween() {
        String expected = "date BETWEEN 1000 AND 2000";

        Example test = Example.of(JsonObject.create().put("date", JsonArray.from(1000, 2000)),
                Collections.singletonMap("date", BETWEEN));

        assertThat(test.toN1ql().toString())
                .isEqualTo(expected);
    }

    @Test
    public void testNotBetween() {
        String expected = "date NOT BETWEEN 1000 AND 2000";

        Example test = Example.of(JsonObject.create().put("date", JsonArray.from(1000, 2000)),
                Collections.singletonMap("date", NOT_BETWEEN));

        assertThat(test.toN1ql().toString())
                .isEqualTo(expected);
    }

    @Test
    public void testAllUnaryPositiveOperators() {
        String expected = "a = \"foo\"" +
                " AND b > 1" +
                " AND c >= 2" +
                " AND d < 4" +
                " AND e <= 3" +
                " AND f LIKE \"%toto%\"" +
                " AND g IS NULL" +
                " AND h IS VALUED" +
                " AND i IS MISSING" +
                " AND \"bar\" IN j";

        Map<String, Operator> operators = new HashMap<String, Operator>();
        operators.put("a", EQUALS);
        operators.put("b", GREATER_THAN);
        operators.put("c", GREATER_THAN_EQUALS);
        operators.put("d", LESSER_THAN);
        operators.put("e", LESSER_THAN_EQUALS);
        operators.put("f", LIKE);
        operators.put("g", IS_NULL);
        operators.put("h", IS_VALUED);
        operators.put("i", IS_MISSING);
        operators.put("j", CONTAINS);

        JsonObject values = JsonObject.create()
            .put("a", "foo")
            .put("b", 1)
            .put("c", 2)
            .put("d", 4)
            .put("e", 3)
            .put("f", "%toto%")
            .put("g", (String) null)
            .put("h", (String) null)
            .put("i", (String) null)
            .put("j", "bar");

        Example test = Example.of(values, operators);

        assertThat(test.toN1ql().toString()).isEqualTo(expected);
    }

    @Test
    public void testAllUnaryNegativeOperators() {
        String expected = "a != \"foo\"" +
                " AND f NOT LIKE \"%toto%\"" +
                " AND g IS NOT NULL" +
                " AND h IS NOT VALUED" +
                " AND i IS NOT MISSING" +
                " AND \"bar\" NOT IN j";

        Map<String, Operator> operators = new HashMap<String, Operator>();
        operators.put("a", NOT_EQUALS);
        operators.put("f", NOT_LIKE);
        operators.put("g", IS_NOT_NULL);
        operators.put("h", IS_NOT_VALUED);
        operators.put("i", IS_NOT_MISSING);
        operators.put("j", NOT_CONTAINS);

        JsonObject values = JsonObject.create()
            .put("a", "foo")
            .put("f", "%toto%")
            .put("g", (String) null)
            .put("h", (String) null)
            .put("i", (String) null)
            .put("j", "bar");

        Example test = Example.of(values, operators);

        assertThat(test.toN1ql().toString()).isEqualTo(expected);
    }
}