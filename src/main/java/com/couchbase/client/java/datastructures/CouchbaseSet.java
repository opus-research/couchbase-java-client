package com.couchbase.client.java.datastructures;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.CouchbaseAsyncBucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.subdoc.SubdocOperationResult;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;

public class CouchbaseSet<E> {

    private final String docId;
    private final CouchbaseAsyncBucket bucket;

    public CouchbaseSet(CouchbaseAsyncBucket bucket, String docId) {
        this.bucket = bucket;
        this.docId = docId;
    }

    private Observable<JsonArrayDocument> createDocument(E element) {
        return bucket.upsert(JsonArrayDocument.create(docId, JsonArray.create().add(element)));
    }

    public Observable<Boolean> add(final E element) {
        return add(element, MutationOptionBuilder.build());
    }

    public Observable<Boolean> add(final E element, final MutationOptionBuilder optionBuilder) {
        return Observable.defer(new Func0<Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call() {
                return subdocAddUnique(element, optionBuilder.cas, optionBuilder.persistTo,
                        optionBuilder.replicateTo, optionBuilder.expiry)
                        .map(new Func1<DocumentFragment<Mutation>, Boolean>() {
                            @Override
                            public Boolean call(DocumentFragment<Mutation> documentFragment) {
                                ResponseStatus status = documentFragment.status(0);
                                if (status == ResponseStatus.SUCCESS) {
                                    return true;
                                } else {
                                    if (status == ResponseStatus.SUBDOC_PATH_EXISTS) {
                                        return false;
                                    } else if (status == ResponseStatus.TOO_BIG) {
                                        throw new IllegalStateException("Set full");
                                    } else {
                                        throw new CouchbaseException(status.toString());
                                    }
                                }
                            }
                        });
            }
        });
    }

    private Observable<DocumentFragment<Mutation>> subdocAddUnique(final E element,
                                                                   final long cas,
                                                                   final PersistTo persistTo,
                                                                   final ReplicateTo replicateTo,
                                                                   final int expiry) {
        return bucket.mutateIn(docId).arrayAddUnique("", element, false)
                .withCas(cas)
                .withDurability(persistTo, replicateTo)
                .withExpiry(expiry)
                .execute()
                .onErrorResumeNext(new Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                    @Override
                    public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                        if (throwable instanceof DocumentDoesNotExistException) {
                            return createDocument(element)
                                    .map(new Func1<JsonArrayDocument, DocumentFragment<Mutation>>() {
                                        @Override
                                        public DocumentFragment<Mutation> call(JsonArrayDocument document) {
                                            List<SubdocOperationResult<Mutation>> list;
                                            list = new ArrayList<SubdocOperationResult<Mutation>>();
                                            list.add(SubdocOperationResult.createResult(null, Mutation.ARRAY_ADD_UNIQUE,
                                                    ResponseStatus.SUCCESS, element));
                                            return new DocumentFragment<Mutation>(document.id(), document.cas(),
                                                    document.mutationToken(),
                                                    list);
                                        }
                                    }).onErrorResumeNext(new Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                                        @Override
                                        public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                                            if (throwable instanceof DocumentAlreadyExistsException) {
                                                return subdocAddUnique(element, cas, persistTo, replicateTo, expiry);
                                            } else {
                                                return Observable.error(throwable);
                                            }
                                        }
                                    });
                        } else {
                            return Observable.error(throwable);
                        }
                    }
                });
    }

    public Observable<Boolean> exists(final E element) {
        return Observable.defer(new Func0<Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call() {
                return bucket.get(docId, JsonArrayDocument.class)
                        .map(new Func1<JsonArrayDocument, Boolean>() {
                            @Override
                            public Boolean call(JsonArrayDocument document) {
                                JsonArray jsonArray = document.content();
                                Iterator<Object> iterator = jsonArray.iterator();
                                while (iterator.hasNext()) {
                                    if (iterator.next().equals(element)) {
                                        return true;
                                    }
                                }
                                return false;
                            }
                        });
            }
        });
    }

    public Observable<Boolean> remove(final E element) {
        return remove(element, MutationOptionBuilder.build());
    }

    public Observable<Boolean> remove(final E element, MutationOptionBuilder mutationOptionBuilder) {
        return new CouchbaseList<E>(bucket, docId).remove(element, mutationOptionBuilder);
    }

    public Observable<Integer> size() {
        return new CouchbaseList<E>(bucket, docId).size();
    }
}