package com.couchbase.client.protocol.n1ql;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;
import org.codehaus.jettison.json.JSONObject;

import java.util.logging.Level;
import java.util.logging.Logger;

public class N1qlResponseHandler extends AsyncCompletionHandler<N1qlResponse> {
    private static final Logger LOGGER = Logger.getLogger(N1qlResponseHandler.class.getName());

    @Override
    public N1qlResponse onCompleted(Response response) throws Exception {
        JSONObject json = new JSONObject(response.getResponseBody());
        LOGGER.log(Level.INFO, "Retrieved JSON response: " + json);
        return new N1qlResponse().parseJson(json);
    }
}
