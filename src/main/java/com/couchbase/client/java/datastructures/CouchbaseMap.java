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
import com.couchbase.client.java.*;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.*;

/**
 * CouchbaseMap is a data structure backed by a JsonDocument and has a map collection like interface supporting
 * map operations that can be executed synchronously against a Couchbase Server bucket
 * Here key is a string and value can be a Json data type.
 *
 * @param <V> Type of the value
 * @author Subhashni Balakrishnan
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class CouchbaseMap<V> {

	private final String docId;
	private final Bucket bucket;
	private final Class<V> type;

	/**
	 * Create {@link Bucket Couchbase-backed} CouchbaseMap, backed by the document identified by <code>id</code>
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
	public CouchbaseMap(Bucket bucket, String docId, Class<V> type) {
		this.bucket = bucket;
		this.docId = docId;
		this.type = type;
		if (!bucket.exists(docId)) {
			bucket.insert(JsonDocument.create(docId, JsonObject.create()));
		}
	}

	/**
	 * Add a key value pair into CouchbaseMap
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
	 * @param key key to be stored
	 * @param value value to be stored
	 * @return true if successful
	 */
	public Boolean add(final String key, final V value) {
		return this.bucket.mapAdd(this.docId, key, value);
	}

	/**
	 * Add a key value pair into CouchbaseMap with additional mutation options provided by {@link MutationOptionBuilder}
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
	 * @param key key to be stored
	 * @param value value to be stored
	 * @param mutationOptionBuilder mutation options {@link MutationOptionBuilder}
	 * @return true if successful
	 */
	public Boolean add(final String key, final V value, final MutationOptionBuilder mutationOptionBuilder) {
		return this.bucket.mapAdd(this.docId, key, value, mutationOptionBuilder);
	}

	/**
	 * Get value of a key in the CouchbaseMap
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
	 * @param key key in the map
	 * @return value if found
	 */
	public V get(final String key) {
		return this.bucket.mapGet(this.docId, key, type);
	}

	/**
	 * Remove a key value pair from CouchbaseMap
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
	 * @param key key to be removed
	 * @return true if successful, even if the key doesn't exist
	 */
	public Boolean remove(final String key) {
		return this.bucket.mapRemove(this.docId, key);
	}

	/**
	 * Remove a key value pair from CouchbaseMap with additional mutation options provided by {@link MutationOptionBuilder}
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
	 * @param key key to be removed
	 * @return true if successful, even if the key doesn't exist
	 */
	public Boolean remove(final String key, final MutationOptionBuilder mutationOptionBuilder) {
		return this.bucket.mapRemove(this.docId, key, mutationOptionBuilder);
	}

	/**
	 * Returns the number key value pairs in CouchbaseMap
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
	 * @return number of key value pairs
	 */
	public int size() {
		return this.bucket.mapSize(this.docId);
	}
}