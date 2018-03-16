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
package com.couchbase.client.java.cluster.api;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.config.RestApiRequest;
import com.couchbase.client.core.message.config.RestApiResponse;
import com.couchbase.client.core.utils.Blocking;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpHeaders;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.deps.io.netty.handler.timeout.TimeoutException;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.json.JsonValue;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.query.core.N1qlQueryExecutor;
import rx.Observable;
import rx.functions.Func0;

/**
 * An utility method to execute generic HTTP calls on a cluster's
 * REST API.
 *
 * @author Simon Baslé
 * @since 2.3.2
 */
public class ClusterApiClient {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(N1qlQueryExecutor.class);
    private static final TimeUnit DEFAULT_TIMEUNIT = TimeUnit.MILLISECONDS;

    private final String login;
    private final String password;
    private final ClusterFacade core;
    private final CouchbaseEnvironment environment;
    private final long defaultTimeout;

    /**
     * Build a new {@link ClusterApiClient} to work with a given {@link ClusterFacade}.
     *
     * @param username the login to use for REST api calls (eg. administrative username).
     * @param password the password associated with the username.
     * @param core the {@link ClusterFacade} through which to sen requests.
     * @param environment the environment used to configure the client (eg. default synchronous timeout).
     */
    public ClusterApiClient(String username, String password, ClusterFacade core,
            CouchbaseEnvironment environment) {
        this.login = username;
        this.password = password;
        this.core = core;
        this.environment = environment;
        this.defaultTimeout = environment.viewTimeout();
    }

    /**
     * Prepare a GET request for the cluster API on a given path.
     *
     * The elements of the path are processed as follows:
     *  - if an element starts with a slash, it is kept. Otherwise a trailing slash is added.
     *  - if an element ends with a slash, it is removed.
     *  - if an element is null, it is ignored.
     *
     * @param paths the elements of the path.
     * @return a {@link RestBuilder} used to further configure the request. Use its
     *   {@link RestBuilder#execute()} methods to trigger the request.
     */
    public RestBuilder get(String... paths) {
        return new RestBuilder(HttpMethod.GET, buildPath(paths));
    }

    /**
     * Prepare a POST request for the cluster API on a given path.
     *
     * The elements of the path are processed as follows:
     *  - if an element starts with a slash, it is kept. Otherwise a trailing slash is added.
     *  - if an element ends with a slash, it is removed.
     *  - if an element is null, it is ignored.
     *
     * @param paths the elements of the path.
     * @return a {@link RestBuilder} used to further configure the request. Use its
     *   {@link RestBuilder#execute()} methods to trigger the request.
     */
    public RestBuilder post(String... paths) {
        return new RestBuilder(HttpMethod.POST, buildPath(paths));
    }

    /**
     * Prepare a PUT request for the cluster API on a given path.
     *
     * The elements of the path are processed as follows:
     *  - if an element starts with a slash, it is kept. Otherwise a trailing slash is added.
     *  - if an element ends with a slash, it is removed.
     *  - if an element is null, it is ignored.
     *
     * @param paths the elements of the path.
     * @return a {@link RestBuilder} used to further configure the request. Use its
     *   {@link RestBuilder#execute()} methods to trigger the request.
     */
    public RestBuilder put(String... paths) {
        return new RestBuilder(HttpMethod.PUT, buildPath(paths));
    }

    /**
     * Prepare a DELETE request for the cluster API on a given path.
     *
     * The elements of the path are processed as follows:
     *  - if an element starts with a slash, it is kept. Otherwise a trailing slash is added.
     *  - if an element ends with a slash, it is removed.
     *  - if an element is null, it is ignored.
     *
     * @param paths the elements of the path.
     * @return a {@link RestBuilder} used to further configure the request. Use its
     *   {@link RestBuilder#execute()} methods to trigger the request.
     */
    public RestBuilder delete(String... paths) {
        return new RestBuilder(HttpMethod.DELETE, buildPath(paths));
    }

    /**
     * Assemble path elements to form an HTTP path:
     *  - if an element starts with a slash, it is kept. Otherwise a trailing slash is added.
     *  - if an element ends with a slash, it is removed.
     *  - if an element is null, it is ignored.
     *
     * @param paths the elements of the path.
     * @returns the full path.
     */
    public static String buildPath(String... paths) {
        if (paths == null || paths.length == 0) {
            throw new IllegalArgumentException();
        }

        StringBuilder path = new StringBuilder();
        for (int i = 0; i < paths.length; i++) {
            String p = paths[i];
            if (p == null) continue;

            //skip separator if one already present
            if (p.charAt(0) != '/') {
                path.append('/');
            }
            //remove trailing /
            if (p.charAt(p.length() - 1) == '/') {
                path.append(p, 0, p.length() - 1);
            } else {
                path.append(p);
            }
        }
        return path.toString();
    }

    /**
     * A builder class to incrementally construct REST API requests and execute
     * them.
     */
    public class RestBuilder {

        private final HttpMethod method;
        private final String path;

        private final Map<String, String> params;
        private final Map<String, Object> headers;

        private String body;

        /**
         * @param method the {@link HttpMethod} for the request.
         * @param path the full URL path for the request.
         */
        public RestBuilder(HttpMethod method, String path) {
            this.method = method;
            this.path = path;
            this.body = "";

            this.params = new LinkedHashMap<String, String>();
            this.headers = new LinkedHashMap<String, Object>();
        }

        /**
         * Adds an URL query parameter to the request. Using a key twice will
         * result in the last call being taken into account.
         *
         * @param key the parameter key.
         * @param value the parameter value.
         */
        public RestBuilder withParam(String key, String value) {
            this.params.put(key, value);
            return this;
        }

        /**
         * Adds an HTTP header to the request. Using a key twice will result
         * in the last value being used for a given header.
         *
         * @param key the header name (see {@link HttpHeaders.Names} for standard names).
         * @param value the header value (see {@link HttpHeaders.Values} for standard values).
         */
        public RestBuilder withHeader(String key, Object value) {
            this.headers.put(key, value);
            return this;
        }

        /**
         * Sets the body for the request without assuming a Content-Type or Accept header.
         * Note that you should avoid calling this for HTTP methods where it makes no sense
         * (eg. GET, DELETE), as it won't be ignored for these types of requests.
         *
         * @param body the raw body value to use, as a String.
         */
        public RestBuilder bodyRaw(String body) {
            this.body = body;
            return this;
        }

        /**
         * Sets the "Content-Type" standard header's value. This is a convenience
         * method equivalent to calling
         * {@link #withHeader(String, Object) withHeader("Content-Type", type)}.
         *
         * @param type the "Content-Type" to use.
         */
        public RestBuilder contentType(String type) {
            return withHeader(HttpHeaders.Names.CONTENT_TYPE, type);
        }

        /**
         * Sets the "Accept" standard header's value. This is a convenience
         * method equivalent to calling {@link #withHeader(String, Object) withHeader("Accept", type)}.
         *
         * @param type the "Accept" type to use.
         */
        public RestBuilder accept(String type) {
            return withHeader(HttpHeaders.Names.ACCEPT, type);
        }

        /**
         * Sets the body for the request, assuming it is JSON. This is equivalent to setting
         * the {@link #contentType(String) "Content-Type"} to <code>"application/json"</code>
         * and then setting the body via {@link #bodyRaw(String)}.
         *
         * Note that you should avoid calling this for HTTP methods where it makes no sense
         * (eg. GET, DELETE), as it won't be ignored for these types of requests.
         *
         * @param jsonBody the JSON body to use, as a String.
         */
        public RestBuilder body(String jsonBody) {
            accept(HttpHeaders.Values.APPLICATION_JSON);
            contentType(HttpHeaders.Values.APPLICATION_JSON);
            bodyRaw(jsonBody);
            return this;
        }

        /**
         * Sets the body for the request, assuming it is JSON. This is equivalent to setting
         * the {@link #contentType(String) "Content-Type"} to <code>"application/json"</code>
         * and then setting the body via {@link #bodyRaw(String)}.
         *
         * Note that you should avoid calling this for HTTP methods where it makes no sense
         * (eg. GET, DELETE), as it won't be ignored for these types of requests.
         *
         * @param jsonBody the JSON body to use, as a {@link JsonObject}.
         */
        public RestBuilder body(JsonValue jsonBody) {
            return body(jsonBody.toString());
        }

        /**
         * Sets the body for the request to be an url-encoded form. This is equivalent to setting
         * the {@link #contentType(String) "Content-Type"} to <code>"application/x-www-form-urlencoded"</code>
         * and then setting the body via {@link #bodyRaw(String)}.
         *
         * @param form the {@link Form} builder object used to set form parameters.
         */
        public RestBuilder bodyForm(Form form) {
            contentType(HttpHeaders.Values.APPLICATION_X_WWW_FORM_URLENCODED);
            return bodyRaw(form.toUrlEncodedString());
        }

        //==== Getters ====

        /**
         * @return the {@link HttpMethod} used for this request.
         */
        public HttpMethod method() {
            return method;
        }

        /**
         * @return the full HTTP path (minus query parameters) used for this request.
         */
        public String path() {
            return this.path;
        }

        /**
         * @return the body used for this request.
         */
        public String body() {
            return this.body;
        }

        /**
         * @return a copy of the query parameters used for this request.
         */
        public Map<String, String> params() {
            return new LinkedHashMap<String, String>(params);
        }

        /**
         * @return a copy of the HTTP headers used for this request.
         */
        public Map<String, Object> headers() {
            return new LinkedHashMap<String, Object>(headers);
        }

        //==== RestApiRequest and execution ====

        /**
         * @return the {@link RestApiRequest} message sent through the {@link ClusterFacade}
         * when executing this request.
         */
        public RestApiRequest asRequest() {
            return new RestApiRequest(
                    ClusterApiClient.this.login, ClusterApiClient.this.password,
                    this.method, this.path, this.params, this.headers, this.body
            );
        }

        /**
         * Executes the API request in an asynchronous fashion.
         *
         * The return type is an {@link Observable} that will only emit the result of executing the request.
         * It is a cold Observable (and the request is only sent when it is subscribed to).
         *
         * @return an {@link Observable} of the result of the API call, which is a {@link RestApiResponse}.
         */
        public Observable<RestApiResponse> executeAsync() {
            return Observable.defer(new Func0<Observable<RestApiResponse>>() {
                @Override
                public Observable<RestApiResponse> call() {
                    RestApiRequest apiRequest = asRequest();
                    LOGGER.debug("Executing Cluster API request {} on {}", apiRequest.method(), apiRequest.pathWithParameters());
                    return core.send(asRequest());
                }
            });
        }

        /**
         * Executes the API request in a synchronous fashion, using the given timeout.
         *
         * @param timeout the custom timeout to use for the request.
         * @param timeUnit the {@link TimeUnit} for the timeout.
         * @return the result of the API call, as a {@link RestApiResponse}.
         * @throws RuntimeException wrapping a {@link TimeoutException} in case the request took too long.
         */
        public RestApiResponse execute(long timeout, TimeUnit timeUnit) {
            return Blocking.blockForSingle(executeAsync(), timeout, timeUnit);
        }

        /**
         * Executes the API request in a synchronous fashion, using the default timeout.
         *
         * The default timeout is currently the same as the {@link CouchbaseEnvironment#viewTimeout() view timeout}.
         *
         * @return the result of the API call, as a {@link RestApiResponse}.
         * @throws RuntimeException wrapping a {@link TimeoutException} in case the request took too long.
         */
        public RestApiResponse execute() {
            return execute(defaultTimeout, DEFAULT_TIMEUNIT);
        }
    }

}
