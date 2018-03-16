package com.couchbase.client;

import com.couchbase.client.protocol.n1ql.N1qlResponse;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;

/**
 * Created by setodd on 9/3/14.
 */
public class CouchbaseN1qlClientTest {
    private static final String HOST = "localhost";
    private static int PORT = 8093;
    private static CouchbaseN1qlClientIF client;

    @BeforeClass
    public static void setupTest() {
        client = new CouchbaseN1qlClient(HOST, PORT);
    }

    @AfterClass
    public static void tearDown() {
        client.close();
    }

    @Test
    public void testSelectGreeting() throws InterruptedException, ExecutionException, IOException, JSONException {
        N1qlResponse response = client.query("SELECT 'Hello World' AS Greeting");
        assertTrue(response.getResultCount() == 1);
        JSONObject result = response.getResults().get(0);
        assertTrue(result.get("Greeting").equals("Hello World"));
    }

}
