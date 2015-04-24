/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core.policies;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.Beta;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.PerHostPercentileTracker;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;

@Beta
public class PercentileSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {
    private final PerHostPercentileTracker percentileTracker;
    private final double percentile;
    private final int maxSpeculativeExecutions;

    public PercentileSpeculativeExecutionPolicy(PerHostPercentileTracker percentileTracker, double percentile, int maxSpeculativeExecutions) {
        this.percentileTracker = percentileTracker;
        this.percentile = percentile;
        this.maxSpeculativeExecutions = maxSpeculativeExecutions;
    }

    @Override
    public SpeculativeExecutionPlan newPlan(String loggedKeyspace, Statement statement) {
        return new SpeculativeExecutionPlan() {
            private final AtomicInteger remaining = new AtomicInteger(maxSpeculativeExecutions);

            @Override
            public long nextExecution(Host lastQueried) {
                if (remaining.getAndDecrement() > 0)
                    return percentileTracker.getLatencyAtPercentile(lastQueried, percentile);
                else
                    return -1;
            }
        };
    }

    @Override
    public void init(Cluster cluster) {
        // nothing
    }

    @Override
    public void close() {
        // nothing
    }
}
