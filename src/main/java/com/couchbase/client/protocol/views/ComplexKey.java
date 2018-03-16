package com.couchbase.client.protocol.views;

import java.util.*;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

/**
 * Allows simple definition of complex JSON keys for query inputs.
 *
 * This was inspired by the Ektorp project, which queries Apache CouchDB.
 */
public class ComplexKey {

	private final List<Object> components;

	private static final Object EMPTY_OBJECT = new Object();
	private static final Object[] EMPTY_ARRAY = new Object[0];

	/**
   * Generate a ComplexKey based on the input Object arguments (varargs).
   *
   * This method is most often used along with the Query object and done
   * when new a complex key is used as a query input.  For example, to query
   * with the array of integers 2012, 9, 5 (a common method of setting up
   * reduceable date queries) one may do something like:
   *
   * ComplexKey.of(2012, 9, 5);
   *
   * @param components
   * @return
   */
  public static ComplexKey of(Object... components) {
		return new ComplexKey(components);
	}

	/**
   *
   * @return
   */
  public static Object emptyObject() {
		return EMPTY_OBJECT;
	}

	/**
   *
   * @return
   */
  public static Object[] emptyArray() {
		return EMPTY_ARRAY;
	}

	private ComplexKey(Object[] components) {
		this.components = Arrays.asList(components);
	}

	/**
   * Generate a JSON string of the ComplexKey.
   *
   * @return the JSON of the underlying complex key
   */
  public String toJson() {
    JSONArray key = new JSONArray();
		for (Object component : components) {
			if (component == EMPTY_OBJECT) {
        key.put(new JSONObject());
			} else {
				key.put(component);
			}
		}
		return key.toString();
	}
}