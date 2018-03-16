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

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.error.*;

/**
 * CouchbaseList data structure backed by JsonArrayDocument and has a list collection like interface supporting
 * list operations that can be executed synchronously against a Couchbase Server bucket.
 *
 * @param <E> Type of element
 * @author Subhashni Balakrishnan
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class CouchbaseList<E> {

	private final String docId;
	private final Bucket bucket;
	private final Class<E> type;

	/**
	 * Create {@link Bucket Couchbase-backed} CouchbaseList, backed by the document identified by <code>id</code>
	 * in <code>bucket</code>. Note that if the document with the data structure already exists,
	 * its content will be used as initial content.
	 *
	 * This method throws under the following conditions:
	 * - The producer outpaces the SDK: {@link BackpressureException}
	 * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
	 * retrying: {@link RequestCancelledException}
	 * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
	 * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
	 * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
	 *
	 * @param bucket the {@link Bucket} through which to interact with the document.
	 * @param docId the id of the Couchbase document to back the list.
	 * @param type generic class type
	 */
	public CouchbaseList(Bucket bucket, String docId, Class<E> type) {
		this.bucket = bucket;
		this.docId = docId;
		this.type = type;
		if (!bucket.exists(docId)) {
			bucket.insert(JsonArrayDocument.create(docId, JsonArray.create()));
		}
	}

	/**
	 * Get element at an index in the CouchbaseList
	 *
	 * This method throws under the following conditions:
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
	public E get(final int index) {
		return this.bucket.listGet(this.docId, index, this.type);
	}

	/**
	 * Push an element to tail of CouchbaseList
	 *
	 * This method throws under the following conditions:
	 * - The producer outpaces the SDK: {@link BackpressureException}
	 * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
	 * retrying: {@link RequestCancelledException}
	 * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
	 * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
	 * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
	 * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
	 *
	 * @param element element to be pushed into the queue
	 * @return true if successful
	 */
	public boolean add(final E element) {
		return this.bucket.listAppend(this.docId, element);
	}

	/**
	 * Push an element to tail of CouchbaseList with additional mutation options provided by {@link MutationOptionBuilder}
	 *
	 * This method throws under the following conditions:
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
	 * @param element element to be pushed into the queue
	 * @param mutationOptionBuilder mutation options {@link MutationOptionBuilder}
	 * @return true if successful
	 */
	public boolean add(final E element, final MutationOptionBuilder mutationOptionBuilder) {
		return this.bucket.listAppend(this.docId, mutationOptionBuilder);
	}

	/**
	 * Remove an element from an index in CouchbaseList
	 *
	 * This method throws under the following conditions:
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
	public boolean remove(int index) {
		return this.bucket.listRemove(this.docId, index);
	}

	/**
	 * Remove an element from an index in CouchbaseList with additional mutation options provided by
	 * {@link MutationOptionBuilder}.
	 *
	 * This method throws under the following conditions:
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
	public boolean remove(final int index, final MutationOptionBuilder mutationOptionBuilder) {
		return this.bucket.listRemove(this.docId, index, mutationOptionBuilder);
	}

	/**
	 * Add an element at an index in CouchbaseList.
	 *
	 * This method throws under the following conditions:
	 * - The producer outpaces the SDK: {@link BackpressureException}
	 * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
	 * retrying: {@link RequestCancelledException}
	 * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
	 * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
	 * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
	 * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
	 *
	 * @param index index in the list
	 * @param element element to be added
	 * @return true if successful
	 */
	public boolean set(int index, final E element) {
		return this.bucket.listSet(this.docId, index, element);
	}

	/**
	 * Add an element at an index in CouchbaseList with additional mutation options provided by
	 * {@link MutationOptionBuilder}.
	 *
	 * This method throws under the following conditions:
	 * - The producer outpaces the SDK: {@link BackpressureException}
	 * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
	 * retrying: {@link RequestCancelledException}
	 * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
	 * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
	 * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
	 * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
	 *
	 * @param index index in the list
	 * @param element element to be added
	 * @return true if successful
	 */
	public boolean set(int index, final E element, MutationOptionBuilder mutationOptionBuilder) {
		return this.bucket.listSet(this.docId, index, element, mutationOptionBuilder);
	}

	/**
	 * Returns the number of elements in CouchbaseList
	 *
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
	public int size() {
		return this.bucket.listSize(this.docId);
	}
}