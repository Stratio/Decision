/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.streaming.metrics;

import com.codahale.metrics.*;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.QueryFactory;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class SiddhiStreamReporter extends ScheduledReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SiddhiStreamReporter.class);
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private static final String GAUGE_STREAM_NAME = "streaming-gauge-metrics";
    private static final Map<String, Attribute.Type> GAUGE_PROPERTIES =
            ImmutableMap.<String, Attribute.Type>builder().
                    put("name", Attribute.Type.STRING).
                    put("timestamp", Attribute.Type.DOUBLE).
                    put("value", Attribute.Type.STRING).
                    build();
    private static final String COUNTER_STREAM_NAME = "streaming-counter-metrics";
    private static final Map<String, Attribute.Type> COUNTER_PROPERTIES =
            ImmutableMap.<String, Attribute.Type>builder().
                    put("name", Attribute.Type.STRING).
                    put("timestamp", Attribute.Type.DOUBLE).
                    put("count", Attribute.Type.DOUBLE).
                    build();
    private static final String HISTOGRAM_STREAM_NAME = "streaming-histogram-metrics";
    private static final Map<String, Attribute.Type> HISTOGRAM_PROPERTIES =
            ImmutableMap.<String, Attribute.Type>builder().
                    put("name", Attribute.Type.STRING).
                    put("timestamp", Attribute.Type.DOUBLE).
                    put("count", Attribute.Type.DOUBLE).
                    put("max", Attribute.Type.DOUBLE).
                    put("mean", Attribute.Type.FLOAT).
                    put("min", Attribute.Type.DOUBLE).
                    put("stddev", Attribute.Type.FLOAT).
                    put("p50", Attribute.Type.FLOAT).
                    put("p75", Attribute.Type.FLOAT).
                    put("p95", Attribute.Type.FLOAT).
                    put("p98", Attribute.Type.FLOAT).
                    put("p99", Attribute.Type.FLOAT).
                    put("p999", Attribute.Type.FLOAT).
                    build();
    private static final String METER_STREAM_NAME = "streaming-meter-metrics";
    private static final Map<String, Attribute.Type> METER_PROPERTIES =
            ImmutableMap.<String, Attribute.Type>builder().
                    put("name", Attribute.Type.STRING).
                    put("timestamp", Attribute.Type.DOUBLE).
                    put("count", Attribute.Type.DOUBLE).
                    put("mean_rate", Attribute.Type.FLOAT).
                    put("m1_rate", Attribute.Type.FLOAT).
                    put("m5_rate", Attribute.Type.FLOAT).
                    put("m15_rate", Attribute.Type.FLOAT).
                    put("rate_unit", Attribute.Type.STRING).
                    build();
    private static final String TIMER_STREAM_NAME = "streaming-timer-metrics";
    private static final Map<String, Attribute.Type> TIMER_PROPERTIES =
            ImmutableMap.<String, Attribute.Type>builder().
                    put("name", Attribute.Type.STRING).
                    put("timestamp", Attribute.Type.DOUBLE).
                    put("count", Attribute.Type.DOUBLE).
                    put("max", Attribute.Type.FLOAT).
                    put("mean", Attribute.Type.FLOAT).
                    put("min", Attribute.Type.FLOAT).
                    put("stddev", Attribute.Type.FLOAT).
                    put("p50", Attribute.Type.FLOAT).
                    put("p75", Attribute.Type.FLOAT).
                    put("p95", Attribute.Type.FLOAT).
                    put("p98", Attribute.Type.FLOAT).
                    put("p99", Attribute.Type.FLOAT).
                    put("p999", Attribute.Type.FLOAT).
                    put("mean_rate", Attribute.Type.FLOAT).
                    put("m1_rate", Attribute.Type.FLOAT).
                    put("m5_rate", Attribute.Type.FLOAT).
                    put("m15_rate", Attribute.Type.FLOAT).
                    put("rate_unit", Attribute.Type.STRING).
                    put("duration_unit", Attribute.Type.STRING).
                    build();


    private final SiddhiManager siddhiManager;
    private final Clock clock;

    private SiddhiStreamReporter(MetricRegistry registry,
                                 SiddhiManager siddhiManager,
                                 TimeUnit rateUnit,
                                 TimeUnit durationUnit,
                                 Clock clock,
                                 MetricFilter filter) {
        super(registry, "siddhi-reporter", filter, rateUnit, durationUnit);
        this.siddhiManager = siddhiManager;
        this.clock = clock;

        // Create metric streams
        createStream(GAUGE_STREAM_NAME, GAUGE_PROPERTIES);
        createStream(COUNTER_STREAM_NAME, COUNTER_PROPERTIES);
        createStream(HISTOGRAM_STREAM_NAME, HISTOGRAM_PROPERTIES);
        createStream(METER_STREAM_NAME, METER_PROPERTIES);
        createStream(TIMER_STREAM_NAME, TIMER_PROPERTIES);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        final long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.getTime());

        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            reportGauge(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            reportCounter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            reportHistogram(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            reportMeter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            reportTimer(timestamp, entry.getKey(), entry.getValue());
        }
    }

    private void reportTimer(long timestamp, String name, Timer timer) {
        final Snapshot snapshot = timer.getSnapshot();

        report(TIMER_STREAM_NAME,
                name,
                timestamp,
                timer.getCount(),
                convertDuration(snapshot.getMax()),
                convertDuration(snapshot.getMean()),
                convertDuration(snapshot.getMin()),
                convertDuration(snapshot.getStdDev()),
                convertDuration(snapshot.getMedian()),
                convertDuration(snapshot.get75thPercentile()),
                convertDuration(snapshot.get95thPercentile()),
                convertDuration(snapshot.get98thPercentile()),
                convertDuration(snapshot.get99thPercentile()),
                convertDuration(snapshot.get999thPercentile()),
                convertRate(timer.getMeanRate()),
                convertRate(timer.getOneMinuteRate()),
                convertRate(timer.getFiveMinuteRate()),
                convertRate(timer.getFifteenMinuteRate()),
                getRateUnit(),
                getDurationUnit());
    }

    private void reportMeter(long timestamp, String name, Meter meter) {
        report(METER_STREAM_NAME,
                name,
                timestamp,
                meter.getCount(),
                convertRate(meter.getMeanRate()),
                convertRate(meter.getOneMinuteRate()),
                convertRate(meter.getFiveMinuteRate()),
                convertRate(meter.getFifteenMinuteRate()),
                getRateUnit());
    }

    private void reportHistogram(long timestamp, String name, Histogram histogram) {
        final Snapshot snapshot = histogram.getSnapshot();

        report(HISTOGRAM_STREAM_NAME,
                name,
                timestamp,
                histogram.getCount(),
                snapshot.getMax(),
                snapshot.getMean(),
                snapshot.getMin(),
                snapshot.getStdDev(),
                snapshot.getMedian(),
                snapshot.get75thPercentile(),
                snapshot.get95thPercentile(),
                snapshot.get98thPercentile(),
                snapshot.get99thPercentile(),
                snapshot.get999thPercentile());
    }

    private void reportCounter(long timestamp, String name, Counter counter) {
        report(COUNTER_STREAM_NAME, name, timestamp, counter.getCount());
    }

    private void reportGauge(long timestamp, String name, Gauge gauge) {
        report(GAUGE_STREAM_NAME, name, timestamp, gauge.getValue());
    }

    private void report(String streamName, Object... orderedValues) {
        try {
            siddhiManager.getInputHandler(streamName).send(orderedValues);
        } catch (InterruptedException e) {
            LOGGER.error("Metric event not sended to stream {}", streamName, e);
        }
    }

    private void createStream(String name, Map<String, Attribute.Type> attributes) {
        StreamDefinition stream = QueryFactory.createStreamDefinition().name(name);

        for (Map.Entry<String, Attribute.Type> attribute : attributes.entrySet()) {
            stream.attribute(attribute.getKey(), attribute.getValue());
        }
        siddhiManager.defineStream(stream);
    }


    public static Builder forRegistry(MetricRegistry registry, SiddhiManager siddhiManager) {
        return new Builder(registry, siddhiManager);
    }

    public static class Builder {
        private final SiddhiManager siddhiManager;
        private final MetricRegistry registry;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private Clock clock;
        private MetricFilter filter;

        private Builder(MetricRegistry registry, SiddhiManager siddhiManager) {
            this.siddhiManager = siddhiManager;
            this.registry = registry;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.clock = Clock.defaultClock();
            this.filter = MetricFilter.ALL;
        }

        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public SiddhiStreamReporter build() {
            return new SiddhiStreamReporter(registry, siddhiManager,
                    rateUnit,
                    durationUnit,
                    clock,
                    filter);
        }
    }
}

