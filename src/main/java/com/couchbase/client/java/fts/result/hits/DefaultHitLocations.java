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
package com.couchbase.client.java.fts.result.hits;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

public class DefaultHitLocations implements HitLocations {

    //FIXME synchronize or make concurrent-friendly?
    private final Map<String, Map<String, List<HitLocation>>> locations = new HashMap<String, Map<String, List<HitLocation>>>();
    private volatile int size;

    /**
     * add a location and allow method chaining
     *
     * @param l
     */
    @Override
    public HitLocations add(HitLocation l) {
        //FIXME synchronize or make concurrent-friendly?
        Map<String, List<HitLocation>> byTerm = locations.get(l.field());
        if (byTerm == null) {
            byTerm = new HashMap<String, List<HitLocation>>();
            locations.put(l.field(), byTerm);
        }

        List<HitLocation> list = byTerm.get(l.term());
        if (list == null) {
            list = new ArrayList<HitLocation>();
            byTerm.put(l.term(), list);
        }

        list.add(l);
        size++;
        return this;
    }

    /**
     * list all locations for a given field (any term)
     *
     * @param field
     */
    @Override
    public List<HitLocation> get(String field) {
        Map<String, List<HitLocation>> byTerm = locations.get(field);
        if (byTerm == null) {
            return Collections.emptyList();
        }

        List<HitLocation> result = new LinkedList<HitLocation>();
        for (List<HitLocation> termList : byTerm.values()) {
            result.addAll(termList);
        }
        return result;
    }

    /**
     * list all locations for a given field and term
     *
     * @param field
     * @param term
     */
    @Override
    public List<HitLocation> get(String field, String term) {
        Map<String, List<HitLocation>> byTerm = locations.get(field);
        if (byTerm == null) {
            return Collections.emptyList();
        }

        List<HitLocation> result = byTerm.get(term);
        if (result == null) {
            return Collections.emptyList();
        }
        return new ArrayList<HitLocation>(result);
    }

    /**
     * list all locations (any field, any term)
     */
    @Override
    public List<HitLocation> getAll() {
        List<HitLocation> all = new LinkedList<HitLocation>();
        for (Map.Entry<String, Map<String, List<HitLocation>>> terms : locations.entrySet()) {
            for (List<HitLocation> hitLocations : terms.getValue().values()) {
                all.addAll(hitLocations);
            }
        }
        return all;
    }

    /**
     * size of all()
     */
    @Override
    public long count() {
        return size;
    }

    /**
     * list the fields in this location
     */
    @Override
    public List<String> fields() {
        return new ArrayList(locations.keySet());
    }

    /**
     * list the terms for a given field
     *
     * @param field
     */
    @Override
    public List<String> termsFor(String field) {
        final Map<String, List<HitLocation>> termMap = locations.get(field);
        if (termMap == null) {
            return Collections.emptyList();
        }
        return new ArrayList<String>(termMap.keySet());
    }

    /**
     * list all terms in this locations, considering all fields (so a set)
     */
    @Override
    public Set<String> terms() {
        Set<String> termSet = new HashSet<String>();
        for (Map<String,List<HitLocation>> termMap : locations.values()) {
            termSet.addAll(termMap.keySet());
        }
        return termSet;
    }

    public static HitLocations from(JsonObject locationsJson) {
        DefaultHitLocations hitLocations = new DefaultHitLocations();
        if (locationsJson == null) {
            return hitLocations;
        }

        for (String field : locationsJson.getNames()) {
            JsonObject termsJson = locationsJson.getObject(field);

            for (String term : termsJson.getNames()) {
                JsonArray locsJson = termsJson.getArray(term);

                for (int i = 0; i < locsJson.size(); i++) {
                    JsonObject loc = locsJson.getObject(i);
                    long pos = loc.getLong("pos");
                    long start = loc.getLong("start");
                    long end = loc.getLong("end");
                    JsonArray arrayPositionsJson = loc.getArray("array_positions");
                    long[] arrayPositions = null;
                    if (arrayPositionsJson != null) {
                        arrayPositions = new long[arrayPositionsJson.size()];
                        for (int j = 0; j < arrayPositionsJson.size(); j++) {
                            arrayPositions[j] = arrayPositionsJson.getLong(j);
                        }
                    }
                    hitLocations.add(new HitLocation(field, term, pos, start, end, arrayPositions));
                }
            }
        }
        return hitLocations;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DefaultHitLocations{")
                .append("size=").append(size)
                .append(", locations=[");

        for (Map<String, List<HitLocation>> map : locations.values()) {
            for (List<HitLocation> hitLocations : map.values()) {
                for (HitLocation hitLocation : hitLocations) {
                    sb.append(hitLocation).append(",");
                }
            }
        }

        if (!locations.isEmpty()) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("]}");
        return sb.toString();
    }
}
