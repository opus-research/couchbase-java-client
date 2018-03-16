/**
 * Copyright (C) 2015 Couchbase, Inc.
 * <p/>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p/>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p/>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.client.java.search;

import com.couchbase.client.java.document.json.JsonObject;

/**
 * @author Sergey Avseyev
 */
public class PlanParams {
    public static final int MAX_PARTITIONS_PER_PINDEX = 20;
    public static final int NUM_REPLICAS = 0;
    public static final boolean PLAN_FROZEN = false;

    private final int maxPartitionsPerPIndex;
    private final int numReplicas;
    private final HierarchyRules hierarchyRules;
    private final NodePlanParams nodePlanParams;
    private final boolean planFrozen;

    protected PlanParams(Builder builder) {
        maxPartitionsPerPIndex = builder.maxPartitionsPerPIndex;
        numReplicas = builder.numReplicas;
        hierarchyRules = builder.hierarchyRules;
        nodePlanParams = builder.nodePlanParams;
        planFrozen = builder.planFrozen;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int maxPartitionsPerPIndex() {
        return maxPartitionsPerPIndex;
    }

    public int numReplicas() {
        return numReplicas;
    }

    public HierarchyRules hierarchyRules() {
        return hierarchyRules;
    }

    public NodePlanParams nodePlanParams() {
        return nodePlanParams;
    }

    public boolean planFrozen() {
        return planFrozen;
    }

    public JsonObject json() {
        JsonObject json = JsonObject.create();
        json.put("maxPartitionsPerPIndex", maxPartitionsPerPIndex);
        json.put("numReplicas", numReplicas);
        json.put("planFrozen", planFrozen);
        json.putNull("hierarchyRules");
        json.putNull("nodePlanParams");
        return json;
    }

    public static class Builder {
        public int maxPartitionsPerPIndex = MAX_PARTITIONS_PER_PINDEX;
        public int numReplicas = NUM_REPLICAS;
        public HierarchyRules hierarchyRules = null;
        public NodePlanParams nodePlanParams = null;
        public boolean planFrozen = PLAN_FROZEN;

        protected Builder() {
        }

        public PlanParams build() {
            return new PlanParams(this);
        }

        public Builder maxPartitionsPerPIndex(int maxPartitionsPerPIndex) {
            this.maxPartitionsPerPIndex = maxPartitionsPerPIndex;
            return this;
        }

        public Builder numReplicas(int numReplicas) {
            this.numReplicas = numReplicas;
            return this;
        }

        public Builder hierarchyRules(HierarchyRules hierarchyRules) {
            this.hierarchyRules = hierarchyRules;
            return this;
        }

        public Builder nodePlanParams(NodePlanParams nodePlanParams) {
            this.nodePlanParams = nodePlanParams;
            return this;
        }

        public Builder planFrozen(boolean planFrozen) {
            this.planFrozen = planFrozen;
            return this;
        }
    }

}
