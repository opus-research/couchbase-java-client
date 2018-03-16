package com.couchbase.client.java.view;

import com.couchbase.client.java.document.json.JsonArray;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SpatialViewQueryTest {

    @Test
    public void shouldSetDefaults() {
        SpatialViewQuery query = SpatialViewQuery.from("design", "view");
        assertEquals("design", query.getDesign());
        assertEquals("view", query.getView());
        assertFalse(query.isDevelopment());
        assertTrue(query.toString().isEmpty());
    }

    @Test
    public void shouldSetStartRange() {
        SpatialViewQuery query = SpatialViewQuery.from("design", "view").startRange(JsonArray.from(5.87,47.27,1000));
        assertEquals("start_range=[5.87,47.27,1000]", query.toString());
    }

    @Test
    public void shouldSetEndRange() {
        SpatialViewQuery query = SpatialViewQuery.from("design", "view").endRange(JsonArray.from(15.04, 55.06, null));
        assertEquals("end_range=[15.04,55.06,null]", query.toString());
    }

    @Test
    public void shouldSetRange() {
        SpatialViewQuery query = SpatialViewQuery.from("design", "view")
            .range(JsonArray.from(null, null, 1000), JsonArray.from(null, null, 2000));
        assertEquals("start_range=[null,null,1000]&end_range=[null,null,2000]", query.toString());
    }

    @Test
    public void shouldLimit() {
        SpatialViewQuery query = SpatialViewQuery.from("design", "view").limit(10);
        assertEquals("limit=10", query.toString());
    }

    @Test
    public void shouldSkip() {
        SpatialViewQuery query = SpatialViewQuery.from("design", "view").skip(3);
        assertEquals("skip=3", query.toString());
    }

    @Test
    public void shouldSetStale() {
        SpatialViewQuery query = SpatialViewQuery.from("design", "view").stale(Stale.FALSE);
        assertEquals("stale=false", query.toString());

        query = SpatialViewQuery.from("design", "view").stale(Stale.TRUE);
        assertEquals("stale=ok", query.toString());

        query = SpatialViewQuery.from("design", "view").stale(Stale.UPDATE_AFTER);
        assertEquals("stale=update_after", query.toString());
    }

    @Test
    public void shouldSetOnError() {
        SpatialViewQuery query = SpatialViewQuery.from("design", "view").onError(OnError.CONTINUE);
        assertEquals("on_error=continue", query.toString());

        query = SpatialViewQuery.from("design", "view").onError(OnError.STOP);
        assertEquals("on_error=stop", query.toString());

    }

    @Test
    public void shouldSetDebug() {
        SpatialViewQuery query = SpatialViewQuery.from("design", "view").debug();
        assertEquals("debug=true", query.toString());

        query = SpatialViewQuery.from("design", "view").debug(false);
        assertEquals("debug=false", query.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldDisallowNegativeLimit() {
        SpatialViewQuery.from("design", "view").limit(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldDisallowNegativeSkip() {
        SpatialViewQuery.from("design", "view").skip(-1);
    }

    @Test
    public void shouldToggleDevelopment() {
        SpatialViewQuery query = SpatialViewQuery.from("design", "view").development(true);
        assertTrue(query.isDevelopment());

        query = SpatialViewQuery.from("design", "view").development(false);
        assertFalse(query.isDevelopment());
    }

}