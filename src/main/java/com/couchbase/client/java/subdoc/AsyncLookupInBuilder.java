/*
 * Copyright (C) 2016 Couchbase, Inc.
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

package com.couchbase.client.java.subdoc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.message.kv.subdoc.multi.MultiLookupResponse;
import com.couchbase.client.core.message.kv.subdoc.multi.SubMultiLookupRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SimpleSubdocResponse;
import com.couchbase.client.core.message.kv.subdoc.simple.SubExistRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SubGetRequest;
import com.couchbase.client.deps.io.netty.util.internal.StringUtil;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.TranscodingException;
import com.couchbase.client.java.error.subdoc.DocumentNotJsonException;
import com.couchbase.client.java.error.subdoc.SubDocumentException;
import com.couchbase.client.java.transcoder.subdoc.FragmentTranscoder;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;

/**
 * A builder for subdocument lookups. In order to perform the final set of operations, use the
 * {@link #doLookup()} method. Operations are performed asynchronously (see {@link LookupInBuilder} for a synchronous
 * version).
 *
 * Instances of this builder should be obtained through {@link AsyncBucket#lookupIn(String)} rather than directly
 * constructed.
 *
 * @author Simon Baslé
 * @since 2.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class AsyncLookupInBuilder {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(AsyncLookupInBuilder.class);

    private final ClusterFacade core;
    private final CouchbaseEnvironment environment;
    private final String bucketName;
    private final FragmentTranscoder subdocumentTranscoder;

    private final String docId;

    private final List<LookupSpec> specs;

    /**
     * Instances of this builder should be obtained through {@link AsyncBucket#lookupIn(String)} rather than directly
     * constructed.
    */
    @InterfaceAudience.Private
    public AsyncLookupInBuilder(ClusterFacade core, String bucketName, CouchbaseEnvironment environment,
            FragmentTranscoder transcoder, String docId) {
        if (docId == null || docId.isEmpty()) {
            throw new IllegalArgumentException("The document ID must not be null or empty.");
        }
        if (docId.getBytes().length > 250) {
            throw new IllegalArgumentException("The document ID must not be larger than 250 bytes");
        }

        this.core = core;
        this.bucketName = bucketName;
        this.environment = environment;
        this.subdocumentTranscoder = transcoder;
        this.docId = docId;
        this.specs = new ArrayList<LookupSpec>();
    }

    /**
     * Perform several {@link Lookup lookup} operations inside a single existing {@link JsonDocument JSON document}.
     * The list of path to look for inside the JSON is constructed through builder methods {@link #get(String)} and
     * {@link #exists(String)}.
     *
     * Each spec will receive an answer, overall contained in a {@link DocumentFragment}, meaning that if sub-document
     * level error conditions happen (like the path is malformed or doesn't exist), the whole operation still succeeds.
     *
     * To check for errors, use {@link DocumentFragment#exists(String)} or {@link DocumentFragment#status(String)} (or
     * their index-based variants). Calling {@link DocumentFragment#content(String)} or one of its variants on a failed
     * spec/path will throw the corresponding {@link SubDocumentException}. For successful gets, it will return the
     * value. Note that an exists operation is considered failed if it doesn't return true, so exists operation should
     * usually be checked through {@link DocumentFragment#exists(String)} if you want a boolean result.
     *
     * One special fatal error can also happen that is represented as a LookupResult, when the value couldn't be decoded
     * from JSON. In that case, the ResponseStatus is {@link ResponseStatus#FAILURE} and the value() will throw a
     * {@link TranscodingException}.
     *
     * The subdocument API has the benefit of only transmitting the fragment of the document you work with
     * on the wire, instead of the whole document.
     *
     * This Observable most notable error conditions are:
     *
     *  - The enclosing document does not exist: {@link DocumentDoesNotExistException}
     *  - The enclosing document is not JSON: {@link DocumentNotJsonException}
     *  - No lookup was defined through the builder API: {@link IllegalArgumentException}
     *
     * Other document-level error conditions are similar to those encountered during a document-level {@link AsyncBucket#get(String)}.
     *
     * @return an {@link Observable} of a single {@link DocumentFragment} representing the whole list of results (1 for
     *        each spec), unless a document-level error happened (in which case an exception is propagated).
     */
    public Observable<DocumentFragment<Lookup>> doLookup() {
        if (specs.isEmpty()) {
            throw new IllegalArgumentException("Execution of a subdoc lookup requires at least one operation");
        } else if (specs.size() == 1) {
            //single path optimization
            return doSingleLookup(specs.get(0));
        } else {
            return doMultiLookup();
        }
    }

    /**
     * Get a value inside the JSON document.
     *
     * @param path the path inside the document where to get the value from.
     * @return this builder for chaining.
     */
    public AsyncLookupInBuilder get(String path) {
        if (StringUtil.isNullOrEmpty(path)) {
            throw new IllegalArgumentException("Path is mandatory for subdoc get");
        }
        this.specs.add(new LookupSpec(Lookup.GET, path));
        return this;
    }

    /**
     * Check if a value exists inside the document (if it does not, attempting to get the
     * {@link DocumentFragment#content(int)} will raise an error).
     * This doesn't transmit the value on the wire if it exists, saving the corresponding byte overhead.
     *
     * @param path the path inside the document to check for existence.
     * @return this builder for chaining.
     */
    public AsyncLookupInBuilder exists(String path) {
        if (StringUtil.isNullOrEmpty(path)) {
            throw new IllegalArgumentException("Path is mandatory for subdoc exists");
        }
        this.specs.add(new LookupSpec(Lookup.EXIST, path));
        return this;
    }

    protected Observable<DocumentFragment<Lookup>> doSingleLookup(LookupSpec spec) {
        if (spec.lookup() == Lookup.GET) {
            return getIn(docId, spec.path(), Object.class);
        } else if (spec.lookup() == Lookup.EXIST) {
            return existsIn(docId, spec.path());
        }
        return Observable.error(new UnsupportedOperationException("Lookup type " + spec.lookup() + " unknown"));
    }

    protected Observable<DocumentFragment<Lookup>> doMultiLookup() {
        if (specs.isEmpty()) {
            throw new IllegalArgumentException("At least one Lookup Command is necessary for lookupIn");
        }
        final LookupSpec[] lookupSpecs = specs.toArray(new LookupSpec[specs.size()]);

        return Observable.defer(new Func0<Observable<MultiLookupResponse>>() {
            @Override
            public Observable<MultiLookupResponse> call() {
                return core.send(new SubMultiLookupRequest(docId, bucketName, lookupSpecs));
            }
        }).filter(new Func1<MultiLookupResponse, Boolean>() {
            @Override
            public Boolean call(MultiLookupResponse response) {
                if (response.status().isSuccess() || response.status() == ResponseStatus.SUBDOC_MULTI_PATH_FAILURE) {
                    return true;
                }

                if (response.content() != null && response.content().refCnt() > 0) {
                    response.content().release();
                }

                switch (response.status()) {
                    default:
                        throw SubdocHelper.commonSubdocErrors(response.status(), docId, "MULTI-LOOKUP");
                }
            }
        }).flatMap(new Func1<MultiLookupResponse, Observable<DocumentFragment<Lookup>>>() {
            @Override
            public Observable<DocumentFragment<Lookup>> call(final MultiLookupResponse multiLookupResponse) {
                return Observable.from(multiLookupResponse.responses())
                        .map(new Func1<com.couchbase.client.core.message.kv.subdoc.multi.MultiResult<Lookup>, MultiResult<Lookup>>() {
                            @Override
                            public MultiResult<Lookup> call(com.couchbase.client.core.message.kv.subdoc.multi.MultiResult<Lookup> lookupResult) {
                                String path = lookupResult.path();
                                Lookup operation = lookupResult.operation();
                                ResponseStatus status = lookupResult.status();
                                boolean isExist = operation == Lookup.EXIST;
                                boolean isSuccess = status.isSuccess();
                                boolean isNotFound = status == ResponseStatus.SUBDOC_PATH_NOT_FOUND;

                                try {
                                    if (isExist && isSuccess) {
                                        return MultiResult.createResult(path, operation, status, true);
                                    } else if (isExist && isNotFound) {
                                        return MultiResult.createResult(path, operation, status, false);
                                    } else if (!isExist && isSuccess) {
                                        try {
                                            //generic, so will transform dictionaries into JsonObject and arrays into JsonArray
                                            Object content = subdocumentTranscoder.decode(lookupResult.value(), Object.class);
                                            return MultiResult.createResult(path, operation, status, content);
                                        } catch (TranscodingException e) {
                                            LOGGER.error("Couldn't decode multi-lookup " + operation + " for " + docId + "/" + path, e);
                                            return MultiResult.createFatal(path, operation, e);
                                        }
                                    } else if (!isExist && isNotFound) {
                                        return MultiResult.createResult(path, operation, status, null);
                                    } else {
                                        return MultiResult.createError(path, operation, status, SubdocHelper.commonSubdocErrors(status, docId, path));
                                    }
                                } finally {
                                    if (lookupResult.value() != null) {
                                        lookupResult.value().release();
                                    }
                                }
                            }
                        }).toList()
                        .map(new Func1<List<MultiResult<Lookup>>, DocumentFragment<Lookup>>() {
                            @Override
                            public DocumentFragment<Lookup> call(List<MultiResult<Lookup>> lookupResults) {
                                return new DocumentFragment<Lookup>(docId, 0L, null, lookupResults);
                            }
                        });
            }
        });
    }


    private <T> Observable<DocumentFragment<Lookup>> getIn(final String id, final String path, final Class<T> fragmentType) {
        return Observable.defer(new Func0<Observable<SimpleSubdocResponse>>() {
            @Override
            public Observable<SimpleSubdocResponse> call() {
                SubGetRequest request = new SubGetRequest(id, path, bucketName);
                return core.send(request);
            }
        }).map(new Func1<SimpleSubdocResponse, DocumentFragment<Lookup>>() {
            @Override
            public DocumentFragment<Lookup> call(SimpleSubdocResponse response) {
                if (response.status().isSuccess()) {
                    try {
                        T content = subdocumentTranscoder.decodeWithMessage(response.content(), fragmentType,
                                "Couldn't decode subget fragment for " + id + "/" + path);
                        MultiResult<Lookup> single = MultiResult.createResult(path, Lookup.GET, response.status(), content);
                        return new DocumentFragment<Lookup>(id, response.cas(), response.mutationToken(),
                                Collections.singletonList(single));
                    } finally {
                        if (response.content() != null) {
                            response.content().release();
                        }
                    }
                } else {
                    if (response.content() != null && response.content().refCnt() > 0) {
                        response.content().release();
                    }

                    if (response.status() == ResponseStatus.SUBDOC_PATH_NOT_FOUND) {
                        MultiResult<Lookup> single = MultiResult.createResult(path, Lookup.GET, response.status(), null);
                        return new DocumentFragment<Lookup>(id, response.cas(), response.mutationToken(), Collections.singletonList(single));
                    } else {
                        throw SubdocHelper.commonSubdocErrors(response.status(), id, path);
                    }
                }
            }
        });
    }

    public Observable<DocumentFragment<Lookup>> existsIn(final String id, final String path) {
        return Observable.defer(new Func0<Observable<SimpleSubdocResponse>>() {
            @Override
            public Observable<SimpleSubdocResponse> call() {
                SubExistRequest request = new SubExistRequest(id, path, bucketName);
                return core.send(request);
            }
        }).map(new Func1<SimpleSubdocResponse, DocumentFragment<Lookup>>() {
            @Override
            public DocumentFragment<Lookup> call(SimpleSubdocResponse response) {
                if (response.content() != null && response.content().refCnt() > 0) {
                    response.content().release();
                }

                if (response.status().isSuccess()) {
                    MultiResult<Lookup> result = MultiResult.createResult(path, Lookup.EXIST, response.status(), true);
                    return new DocumentFragment<Lookup>(docId, response.cas(), response.mutationToken(), Collections.singletonList(result));
                } else if (response.status() == ResponseStatus.SUBDOC_PATH_NOT_FOUND) {
                    MultiResult<Lookup> result = MultiResult.createResult(path, Lookup.EXIST, response.status(), false);
                    return new DocumentFragment<Lookup>(docId, response.cas(), response.mutationToken(), Collections.singletonList(result));
                }

                throw SubdocHelper.commonSubdocErrors(response.status(), id, path);
            }
        });
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("lookupIn(").append(docId).append(")[");
        int pos = sb.length();
        for (LookupSpec spec : specs) {
            sb.append(", ").append(spec);
        }
        sb.delete(pos, pos+2);
        sb.append(']');
        return sb.toString();
    }
}
