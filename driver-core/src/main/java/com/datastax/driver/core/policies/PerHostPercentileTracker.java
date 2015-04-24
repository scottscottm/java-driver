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

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.LatencyTracker;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.*;

/**
 * Records per-host request latency percentiles over a sliding time interval.
 */
@Beta
public class PerHostPercentileTracker implements LatencyTracker {
    private static final Logger logger = LoggerFactory.getLogger(PerHostPercentileTracker.class);

    private final ConcurrentMap<Host, Recorder> recorders;
    private final ConcurrentMap<Host, CachedHistogram> cachedHistograms;
    private final long highestTrackableLatencyMillis;
    private final int numberOfSignificantValueDigits;
    private final int minRecordedValues;
    private final long intervalMs;

    /**
     * Builds a new instance.
     *
     * @param highestTrackableLatencyMillis the highest expected latency. If a higher value is reported, it will be ignored and a
     *                                      warning will be logged. A good rule of thumb is to set it slightly higher than
     *                                      {@link com.datastax.driver.core.SocketOptions}
     * @param numberOfSignificantValueDigits the number of significant decimal digits to which histograms will maintain value
     *                                       resolution and separation. Must be a non-negative integer between 0 and 5.
     * @param numberOfHosts the expected number of hosts in the cluster. This is only used to pre-size internal data structures,
     *                      so if the value is not exact it doesn't really matter.
     * @param minRecordedValues the minimum number of data points before we start returning results. If we have less values,
     *                          {@link #getLatencyAtPercentile(Host, double)} will return a negative value. This is used to avoid
     *                          skewed values at startup or if the host was inactive during the last interval.
     * @param interval the time interval used to compute percentiles. For each host, there is a "live" histogram where current
     *                 values are recorded, and a "cached", read-only histogram that is used to fulfill requests. Each time we find
     *                 out that the cached histogram is older than the interval, we switch the two histograms.
     * @param intervalUnit the time unit for the interval.
     */
    public PerHostPercentileTracker(long highestTrackableLatencyMillis, int numberOfSignificantValueDigits,
                                    int numberOfHosts,
                                    int minRecordedValues,
                                    long interval,
                                    TimeUnit intervalUnit) {
        this.highestTrackableLatencyMillis = highestTrackableLatencyMillis;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
        this.minRecordedValues = minRecordedValues;
        this.intervalMs = MILLISECONDS.convert(interval, intervalUnit);
        this.recorders = new MapMaker().initialCapacity(numberOfHosts).makeMap();
        this.cachedHistograms = new MapMaker().initialCapacity(numberOfHosts).makeMap();
    }

    @Override
    public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
        if (!shouldConsiderNewLatency(statement, exception))
            return;

        long latencyMs = NANOSECONDS.toMillis(newLatencyNanos);
        try {
            getRecorder(host).recordValue(latencyMs);
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.warn("Got request with latency of {} ms, which exceeds the configured maximum trackable value {}",
                latencyMs, highestTrackableLatencyMillis);
        }
    }

    /**
     * Returns the request latency for a host at a given percentile.
     *
     * @param host the host.
     * @param percentile the percentile (for example, {@code 99.0} for the 99th percentile).
     * @return the latency (in milliseconds) at the given percentile, or a negative value if it's not available yet.
     */
    public long getLatencyAtPercentile(Host host, double percentile) {
        checkArgument(percentile >= 0.0 && percentile < 100);
        Histogram histogram = getLastIntervalHistogram(host);
        if (histogram == null || histogram.getTotalCount() < minRecordedValues)
            return -1;

        return histogram.getValueAtPercentile(percentile);
    }

    private Recorder getRecorder(Host host) {
        Recorder recorder = recorders.get(host);
        if (recorder == null) {
            recorder = recorders.putIfAbsent(host, new Recorder(highestTrackableLatencyMillis, numberOfSignificantValueDigits));
            // Also set an empty cache entry to remember the time we started recording:
            cachedHistograms.putIfAbsent(host, CachedHistogram.empty());
        }
        return recorder;
    }

    /** @return null if no histogram is available yet (no entries recorded, or not for long enough) */
    private Histogram getLastIntervalHistogram(Host host) {
        try {
            while (true) {
                CachedHistogram entry = cachedHistograms.get(host);
                if (entry == null)
                    return null;

                long age = System.currentTimeMillis() - entry.timestamp;
                if (age < intervalMs) { // current histogram is recent enough
                    return entry.histogram.get();
                } else { // need to refresh
                    Recorder recorder = recorders.get(host);
                    // intervalMs should be much larger than the time it takes to replace a histogram, so this future should never block
                    Histogram staleHistogram = entry.histogram.get(0, MILLISECONDS);
                    SettableFuture<Histogram> future = SettableFuture.create();
                    CachedHistogram newEntry = new CachedHistogram(future);
                    if (cachedHistograms.replace(host, entry, newEntry)) {
                        // Only get the new histogram if we successfully replaced the cache entry.
                        // This ensures that only one thread will do it.
                        Histogram newHistogram = recorder.getIntervalHistogram(staleHistogram);
                        future.set(newHistogram);
                        return newHistogram;
                    }
                    // If we couldn't replace the entry it means we raced, so loop to try again
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new DriverInternalError("Unexpected error", e.getCause());
        } catch (TimeoutException e) {
            throw new DriverInternalError("Unexpected timeout while getting histogram", e);
        }
    }

    static class CachedHistogram {
        final ListenableFuture<Histogram> histogram;
        final long timestamp;

        CachedHistogram(ListenableFuture<Histogram> histogram) {
            this.histogram = histogram;
            this.timestamp = System.currentTimeMillis();
        }

        static CachedHistogram empty() {
            return new CachedHistogram(Futures.<Histogram>immediateFuture(null));
        }
    }

    // TODO this was copy/pasted from LatencyAwarePolicy, maybe it could be refactored as a shared method
    private boolean shouldConsiderNewLatency(Statement statement, Exception exception) {
        // query was successful: always consider
        if (exception == null)
            return true;
        // filter out "fast" errors
        return !EXCLUDED_EXCEPTIONS.contains(exception.getClass());
    }

    /**
     * A set of DriverException subclasses that we should prevent from updating the host's score.
     * The intent behind it is to filter out "fast" errors: when a host replies with such errors,
     * it usually does so very quickly, because it did not involve any actual
     * coordination work. Such errors are not good indicators of the host's responsiveness,
     * and tend to make the host's score look better than it actually is.
     */
    private static final Set<Class<? extends Exception>> EXCLUDED_EXCEPTIONS = ImmutableSet.<Class<? extends Exception>>of(
        UnavailableException.class, // this is done via the snitch and is usually very fast
        OverloadedException.class,
        BootstrappingException.class,
        UnpreparedException.class,
        QueryValidationException.class // query validation also happens at early stages in the coordinator
    );
}
