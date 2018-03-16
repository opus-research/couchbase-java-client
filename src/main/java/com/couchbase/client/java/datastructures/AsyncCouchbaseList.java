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
package com.couchbase.client.java.datastructures;

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseAsyncBucket;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.error.*;
import com.couchbase.client.java.error.subdoc.MultiMutationException;
import com.couchbase.client.java.error.subdoc.PathInvalidException;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.subdoc.SubdocOperationResult;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;

/**
 * CouchbaseList data structure backed by JsonArrayDocument.
 * AsyncCouchbaseList is list collection like interface supporting CouchbaseList operations that can be executed asynchronously
 * against a Couchbase Server bucket.
 *
 * @param <E> Type of element
 * @author subhashni
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class AsyncCouchbaseList<E> {

    private final String docId;
    private final CouchbaseAsyncBucket bucket;

    /**
     * Create {@link Bucket Couchbase-backed} CouchbaseList, backed by the document identified by <code>id</code>
     * in <code>bucket</code>. Note that if the document with the data structure already exists,
     * its content will be used as initial content.
     *
     * @param bucket the {@link Bucket} through which to interact with the document.
     * @param docId the id of the Couchbase document to back the list.
     */
    public AsyncCouchbaseList(CouchbaseAsyncBucket bucket, String docId) {
        this.bucket = bucket;
        this.docId = docId;
    }

    private Observable<JsonArrayDocument> createDocument(final E element) {
        return bucket.upsert(JsonArrayDocument.create(docId, JsonArray.create().add(element)));
    }

    /**
     * Get element at an index in the CouchbaseList.
     * This method throws under the following conditions:
     * - {@link IndexOutOfBoundsException} if index is not found
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param index index in list
     * @return value if found
     */
    public Observable<E> get(final int index) {
        final Func1<DocumentFragment<Lookup>, E> mapResult = new
                Func1<DocumentFragment<Lookup>, E>() {
                    @Override
                    public E call(DocumentFragment<Lookup> documentFragment) {
                        ResponseStatus status = documentFragment.status(0);
                        if (status == ResponseStatus.SUCCESS) {
                            return (E) documentFragment.content(0);
                        } else {
                            if (status == ResponseStatus.SUBDOC_PATH_NOT_FOUND) {
                                throw new IndexOutOfBoundsException();
                            } else {
                                throw new CouchbaseException(status.toString());
                            }
                        }
                    }
                };
        final Func1<Throwable, Observable<? extends E>> convertSubdocException = new
                Func1<Throwable, Observable<? extends E>>() {
                    @Override
                    public Observable<? extends E> call(Throwable throwable) {
                        if (throwable instanceof PathInvalidException) {
                            return Observable.error(new IndexOutOfBoundsException());
                        }
                        return Observable.error(throwable);
                    }
                };

        return Observable.defer(new Func0<Observable<E>>() {
            @Override
            public Observable<E> call() {
                return bucket.lookupIn(docId).get("[" + Integer.toString(index) + "]")
                        .execute()
                        .map(mapResult)
                        .onErrorResumeNext(convertSubdocException);
            }
        });
    }

    /**
     * Push an element to tail of CouchbaseList. If the underlying document for the list does not exist,
     * this operation will create a new document to back the data structure.
     * This method throws under the following conditions:
     * - {@link IllegalStateException} if the list is full (limited by couchbase document size)
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param element element to be pushed into the queue
     * @return true if successful
     */
    public Observable<Boolean> push(final E element) {
        return push(element, MutationOptionBuilder.build());
    }

    /**
     * Push an element to tail of CouchbaseList with additional mutation options provided by {@link MutationOptionBuilder}.
     * If the underlying document for the list does not exist, this operation will create a new document to back
     * the data structure.
     * This method throws under the following conditions:
     * - {@link IllegalStateException} if the list is full (limited by couchbase document size)
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - The durability constraint could not be fulfilled because of a temporary or persistent problem:
     * {@link DurabilityException}.
     * - A CAS value was set and it did not match with the server: {@link CASMismatchException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param element element to be pushed into the queue
     * @param mutationOptionBuilder mutation options {@link MutationOptionBuilder}
     * @return true if successful
     */
    public Observable<Boolean> push(final E element, final MutationOptionBuilder mutationOptionBuilder) {
        return Observable.defer(new Func0<Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call() {
                return subdocPushLast(element, mutationOptionBuilder)
                        .map(getMapResultFnForPushAndShift());
            }
        });
    }

    private Observable<DocumentFragment<Mutation>> subdocPushLast(final E element,
                                                                  final MutationOptionBuilder optionBuilder) {
        final Func1<JsonArrayDocument, DocumentFragment<Mutation>> mapFullDocResulttoSubdoc = new
                Func1<JsonArrayDocument, DocumentFragment<Mutation>>() {
                    @Override
                    public DocumentFragment<Mutation> call(JsonArrayDocument document) {
                        List<SubdocOperationResult<Mutation>> list;
                        list = new ArrayList<SubdocOperationResult<Mutation>>();
                        list.add(SubdocOperationResult.createResult(null, Mutation.ARRAY_PUSH_LAST,
                                ResponseStatus.SUCCESS, element));
                        return new DocumentFragment<Mutation>(document.id(), document.cas(),
                                document.mutationToken(),
                                list);
                    }
                };
        final Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>> retryIfDocExists = new
                Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                    @Override
                    public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                        if (throwable instanceof DocumentAlreadyExistsException) {
                            return subdocPushLast(element, optionBuilder);
                        } else {
                            return Observable.error(throwable);
                        }
                    }
                };
        return bucket.mutateIn(docId).arrayAppend("", element, false)
                .withCas(optionBuilder.cas)
                .withDurability(optionBuilder.persistTo, optionBuilder.replicateTo)
                .withExpiry(optionBuilder.expiry)
                .execute()
                .onErrorResumeNext(new Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                    @Override
                    public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                        if (throwable instanceof DocumentDoesNotExistException) {
                            return createDocument(element)
                                    .map(mapFullDocResulttoSubdoc)
                                    .onErrorResumeNext(retryIfDocExists);
                        } else {
                            if (throwable instanceof MultiMutationException) {
                                ResponseStatus status = ((MultiMutationException) throwable).firstFailureStatus();
                                List<SubdocOperationResult<Mutation>> list;
                                list = new ArrayList<SubdocOperationResult<Mutation>>();
                                list.add(SubdocOperationResult.createResult(null, Mutation.ARRAY_PUSH_LAST, status, null));
                                return Observable.just(new DocumentFragment<Mutation>(null, 0, null, list));
                            } else if (throwable instanceof RequestTooBigException) {
                                List<SubdocOperationResult<Mutation>> list;
                                list = new ArrayList<SubdocOperationResult<Mutation>>();
                                list.add(SubdocOperationResult.createResult(null, Mutation.ARRAY_PUSH_LAST, ResponseStatus.TOO_BIG, null));
                                return Observable.just(new DocumentFragment<Mutation>(null, 0, null, list));
                            } else {
                                return Observable.error(throwable);
                            }
                        }
                    }
                });
    }

    /**
     * Remove an element from an index in CouchbaseList.
     * This method throws under the following conditions:
     * - {@link IndexOutOfBoundsException} if index is not found
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param index index of the element in list
     * @return true if successful
     */
    public Observable<Boolean> remove(int index) {
        return remove(index, MutationOptionBuilder.build());
    }

    /**
     * Remove an element from an index in CouchbaseList with additional mutation options provided by
     * {@link MutationOptionBuilder}.
     * This method throws under the following conditions:
     * - {@link IndexOutOfBoundsException} if index is not found
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
     * - The durability constraint could not be fulfilled because of a temporary or persistent problem:
     * {@link DurabilityException}.
     * - A CAS value was set and it did not match with the server: {@link CASMismatchException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param index index of the element in list
     * @param mutationOptionBuilder mutation options {@link MutationOptionBuilder}
     * @return true if successful
     */
    public Observable<Boolean> remove(final int index, final MutationOptionBuilder mutationOptionBuilder) {
        return Observable.defer(new Func0<Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call() {
                return subdocRemove(index, mutationOptionBuilder)
                        .map(getMapResultFnForRemove());
            }
        });
    }

    private Observable<DocumentFragment<Mutation>> subdocRemove(final int index,
                                                                final MutationOptionBuilder mutationOptionBuilder) {
        return bucket.mutateIn(docId).remove("[" + Integer.toString(index) + "]")
                .withCas(mutationOptionBuilder.cas)
                .withExpiry(mutationOptionBuilder.expiry)
                .withDurability(mutationOptionBuilder.persistTo, mutationOptionBuilder.replicateTo)
                .execute();
    }

    /**
     * Shift list head to element in CouchbaseList. If the underlying document for the list does not exist,
     * this operation will create a new document to back the data structure.
     * This method throws under the following conditions:
     * - {@link IllegalStateException} if the list is full (limited by couchbase document size)
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - The durability constraint could not be fulfilled because of a temporary or persistent problem:
     * {@link DurabilityException}.
     * - A CAS value was set and it did not match with the server: {@link CASMismatchException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param element element to shift as head of list
     * @return true if successful
     */
    public Observable<Boolean> shift(final E element) {
        return shift(element, MutationOptionBuilder.build());
    }

    /**
     * Shift list head to element in CouchbaseList with additional mutation options provided by
     * {@link MutationOptionBuilder}. If the underlying document for the list does not exist,
     * this operation will create a new document to back the data structure.
     * This method throws under the following conditions:
     * - {@link IllegalStateException} if the list is full (limited by couchbase document size)
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - The durability constraint could not be fulfilled because of a temporary or persistent problem:
     * {@link DurabilityException}.
     * - A CAS value was set and it did not match with the server: {@link CASMismatchException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param element element to shift as head of list
     * @param mutationOptionBuilder mutation options {@link MutationOptionBuilder}
     * @return true if successful
     */
    public Observable<Boolean> shift(final E element, final MutationOptionBuilder mutationOptionBuilder) {
        return Observable.defer(new Func0<Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call() {
                return subdocPushFirst(element, mutationOptionBuilder)
                        .map(getMapResultFnForPushAndShift());
            }
        });
    }


    private Observable<DocumentFragment<Mutation>> subdocPushFirst(final E element,
                                                                   final MutationOptionBuilder optionBuilder) {

        final Func1<JsonArrayDocument, DocumentFragment<Mutation>> mapFullDocResultToSubDoc = new
                Func1<JsonArrayDocument, DocumentFragment<Mutation>>() {
                    @Override
                    public DocumentFragment<Mutation> call(JsonArrayDocument document) {
                        List<SubdocOperationResult<Mutation>> list;
                        list = new ArrayList<SubdocOperationResult<Mutation>>();
                        list.add(SubdocOperationResult.createResult(null, Mutation.ARRAY_PUSH_FIRST,
                                ResponseStatus.SUCCESS, element));
                        return new DocumentFragment<Mutation>(document.id(), document.cas(),
                                document.mutationToken(),
                                list);
                    }
                };
        final Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>> retryIfDocExists = new
                Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                    @Override
                    public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                        return subdocPushFirst(element, optionBuilder);
                    }
                };

        return bucket.mutateIn(docId).arrayPrepend("", element, false)
                .withCas(optionBuilder.cas)
                .withDurability(optionBuilder.persistTo, optionBuilder.replicateTo)
                .withExpiry(optionBuilder.expiry)
                .execute()
                .onErrorResumeNext(new Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                    @Override
                    public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                        if (throwable instanceof DocumentDoesNotExistException) {
                            return createDocument(element)
                                    .map(mapFullDocResultToSubDoc)
                                    .onErrorResumeNext(retryIfDocExists);
                        } else {
                            if (throwable instanceof MultiMutationException) {
                                ResponseStatus status = ((MultiMutationException) throwable).firstFailureStatus();
                                List<SubdocOperationResult<Mutation>> list;
                                list = new ArrayList<SubdocOperationResult<Mutation>>();
                                list.add(SubdocOperationResult.createResult(null, Mutation.ARRAY_PUSH_FIRST, status, null));
                                return Observable.just(new DocumentFragment<Mutation>(null, 0, null, list));
                            } else if (throwable instanceof RequestTooBigException) {
                                List<SubdocOperationResult<Mutation>> list;
                                list = new ArrayList<SubdocOperationResult<Mutation>>();
                                list.add(SubdocOperationResult.createResult(null, Mutation.ARRAY_PUSH_FIRST, ResponseStatus.TOO_BIG, null));
                                return Observable.just(new DocumentFragment<Mutation>(null, 0, null, list));
                            } else {
                                return Observable.error(throwable);
                            }
                        }
                    }
                });
    }

    /**
     * Add an element at an index in CouchbaseList. If the underlying document for the list does not exist,
     * this operation will create a new document to back the data structure.
     * This method throws under the following conditions:
     * - {@link IllegalStateException} if the list is full (limited by couchbase document size)
     * - {@link IndexOutOfBoundsException} if index is not valid
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param index index in the list
     * @param element element to be added
     * @return true if successful
     */
    public Observable<Boolean> set(int index, final E element) {
        return set(index, element, MutationOptionBuilder.build());
    }

    /**
     * Add an element at an index in CouchbaseList. with additional mutation options provided by {@link MutationOptionBuilder}.
     * If the underlying document for the list does not exist, this operation will create a new document to back
     * the data structure.
     * This method throws under the following conditions:
     * - {@link IllegalStateException} if the list is full (limited by couchbase document size)
     * - {@link IndexOutOfBoundsException} if index is not valid
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - The durability constraint could not be fulfilled because of a temporary or persistent problem:
     * {@link DurabilityException}.
     * - A CAS value was set and it did not match with the server: {@link CASMismatchException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param index index in the list
     * @param element element to be added
     * @param mutationOptionBuilder mutation options {@link MutationOptionBuilder}
     * @return true if successful
     */
    public Observable<Boolean> set(final int index, final E element, final MutationOptionBuilder mutationOptionBuilder) {
        return Observable.defer(new Func0<Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call() {
                return subdocInsert(index, element, mutationOptionBuilder)
                        .map(getMapResultFnForSet());
            }
        });
    }


    private Observable<DocumentFragment<Mutation>> subdocInsert(final int index,
                                                                final E element,
                                                                final MutationOptionBuilder optionBuilder) {
        final Func1<JsonArrayDocument, DocumentFragment<Mutation>> mapFullDocResultToSubdoc = new
                Func1<JsonArrayDocument, DocumentFragment<Mutation>>() {
                    @Override
                    public DocumentFragment<Mutation> call(JsonArrayDocument document) {
                        List<SubdocOperationResult<Mutation>> list;
                        list = new ArrayList<SubdocOperationResult<Mutation>>();
                        list.add(SubdocOperationResult.createResult(null, Mutation.ARRAY_INSERT, ResponseStatus.SUCCESS, element));
                        return new DocumentFragment<Mutation>(document.id(), document.cas(), document.mutationToken(), list);
                    }
                };

        final Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>> retryIfDocExists = new
                Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                    @Override
                    public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                        if (throwable instanceof DocumentAlreadyExistsException) {
                            return subdocInsert(index, element, optionBuilder);
                        } else {
                            return Observable.error(throwable);
                        }
                    }
                };
        return bucket.mutateIn(docId).arrayInsert("[" + Integer.toString(index) + "]", element)
                .withCas(optionBuilder.cas)
                .withDurability(optionBuilder.persistTo, optionBuilder.replicateTo)
                .withExpiry(optionBuilder.expiry)
                .execute()
                .onErrorResumeNext(new Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                    @Override
                    public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                        if (throwable instanceof DocumentDoesNotExistException) {
                            return createDocument(element)
                                    .map(mapFullDocResultToSubdoc)
                                    .onErrorResumeNext(retryIfDocExists);
                        } else {
                            if (throwable instanceof MultiMutationException) {
                                ResponseStatus status = ((MultiMutationException) throwable).firstFailureStatus();
                                List<SubdocOperationResult<Mutation>> list;
                                list = new ArrayList<SubdocOperationResult<Mutation>>();
                                list.add(SubdocOperationResult.createResult(null, Mutation.ARRAY_INSERT,
                                        status, null));
                                return Observable.just(new DocumentFragment<Mutation>(null, 0,
                                        null,
                                        list));
                            } else if (throwable instanceof RequestTooBigException) {
                                List<SubdocOperationResult<Mutation>> list;
                                list = new ArrayList<SubdocOperationResult<Mutation>>();
                                list.add(SubdocOperationResult.createResult(null, Mutation.ARRAY_INSERT, ResponseStatus.TOO_BIG, null));
                                return Observable.just(new DocumentFragment<Mutation>(null, 0, null, list));
                            } else {
                                return Observable.error(throwable);
                            }
                        }
                    }
                });
    }

    /**
     * Returns the number of elements in CouchbaseList
     * This method throws under the following conditions:
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @return number of elements
     */
    public Observable<Integer> size() {
        return Observable.defer(new Func0<Observable<Integer>>() {
            @Override
            public Observable<Integer> call() {
                return bucket.get(docId, JsonArrayDocument.class)
                        .map(new Func1<JsonArrayDocument, Integer>() {
                            @Override
                            public Integer call(JsonArrayDocument document) {
                                return document.content().size();
                            }
                        });
            }
        });
    }

    private Func1<DocumentFragment<Mutation>, Boolean> getMapResultFnForPushAndShift() {
        return new Func1<DocumentFragment<Mutation>, Boolean>() {
            @Override
            public Boolean call(DocumentFragment<Mutation> documentFragment) {
                ResponseStatus status = documentFragment.status(0);
                if (status == ResponseStatus.SUCCESS) {
                    return true;
                } else {
                    if (status == ResponseStatus.TOO_BIG) {
                        throw new IllegalStateException("List full");
                    } else {
                        throw new CouchbaseException(status.toString());
                    }
                }
            }
        };
    }

    private Func1<DocumentFragment<Mutation>, Boolean> getMapResultFnForRemove() {
        return new Func1<DocumentFragment<Mutation>, Boolean>() {
            @Override
            public Boolean call(DocumentFragment<Mutation> documentFragment) {
                ResponseStatus status = documentFragment.status(0);
                if (status == ResponseStatus.SUCCESS) {
                    return true;
                } else {
                    if (status == ResponseStatus.SUBDOC_PATH_NOT_FOUND || status == ResponseStatus.SUBDOC_PATH_INVALID) {
                        throw new IndexOutOfBoundsException();
                    } else {
                        throw new CouchbaseException(status.toString());
                    }
                }
            }
        };
    }

    private Func1<DocumentFragment<Mutation>, Boolean> getMapResultFnForSet() {
        return new Func1<DocumentFragment<Mutation>, Boolean>() {
            @Override
            public Boolean call(DocumentFragment<Mutation> documentFragment) {
                ResponseStatus status = documentFragment.status(0);
                if (status == ResponseStatus.SUCCESS) {
                    return true;
                } else {
                    if (status == ResponseStatus.SUBDOC_PATH_INVALID) {
                        throw new IndexOutOfBoundsException();
                    } else if (status == ResponseStatus.TOO_BIG) {
                        throw new IllegalStateException("List full");
                    } else {
                        throw new CouchbaseException(status.toString());
                    }
                }
            }
        };
    }
}