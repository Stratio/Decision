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
import com.codahale.metrics.Timer;
import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.exception.ServiceException;
import com.stratio.streaming.service.StreamOperationServiceWithoutMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class SiddhiStreamReporter extends ScheduledReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SiddhiStreamReporter.class);

    private static final String GAUGE_STREAM_NAME = "streaming-gauge-metrics";
    private static final Set<Map.Entry<String, ColumnType>> GAUGE_PROPERTIES = new LinkedHashSet<Map.Entry<String, ColumnType>>() {{
        add(new AbstractMap.SimpleEntry<>("name", ColumnType.STRING));
        add(new AbstractMap.SimpleEntry<>("timestamp", ColumnType.DOUBLE));
        add(new AbstractMap.SimpleEntry<>("value", ColumnType.STRING));
    }};
    private static final String COUNTER_STREAM_NAME = "streaming-counter-metrics";
    private static final Set<Map.Entry<String, ColumnType>> COUNTER_PROPERTIES = new LinkedHashSet<Map.Entry<String, ColumnType>>() {{
        add(new AbstractMap.SimpleEntry<>("name", ColumnType.STRING));
        add(new AbstractMap.SimpleEntry<>("timestamp", ColumnType.DOUBLE));
        add(new AbstractMap.SimpleEntry<>("count", ColumnType.DOUBLE));
    }};
    private static final String HISTOGRAM_STREAM_NAME = "streaming-histogram-metrics";
    private static final Set<Map.Entry<String, ColumnType>> HISTOGRAM_PROPERTIES = new LinkedHashSet<Map.Entry<String, ColumnType>>() {{
        add(new AbstractMap.SimpleEntry<>("name", ColumnType.STRING));
        add(new AbstractMap.SimpleEntry<>("timestamp", ColumnType.DOUBLE));
        add(new AbstractMap.SimpleEntry<>("count", ColumnType.DOUBLE));
        add(new AbstractMap.SimpleEntry<>("max", ColumnType.DOUBLE));
        add(new AbstractMap.SimpleEntry<>("mean", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("min", ColumnType.DOUBLE));
        add(new AbstractMap.SimpleEntry<>("stddev", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("p50", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("p75", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("p95", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("p98", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("p99", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("p999", ColumnType.FLOAT));
    }};
    private static final String METER_STREAM_NAME = "streaming-meter-metrics";
    private static final Set<Map.Entry<String, ColumnType>> METER_PROPERTIES = new LinkedHashSet<Map.Entry<String, ColumnType>>() {{
        add(new AbstractMap.SimpleEntry<>("name", ColumnType.STRING));
        add(new AbstractMap.SimpleEntry<>("timestamp", ColumnType.DOUBLE));
        add(new AbstractMap.SimpleEntry<>("count", ColumnType.DOUBLE));
        add(new AbstractMap.SimpleEntry<>("mean_rate", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("m1_rate", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("m5_rate", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("m15_rate", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("rate_unit", ColumnType.STRING));
    }};
    private static final String TIMER_STREAM_NAME = "streaming-timer-metrics";
    private static final Set<Map.Entry<String, ColumnType>> TIMER_PROPERTIES = new LinkedHashSet<Map.Entry<String, ColumnType>>() {{
        add(new AbstractMap.SimpleEntry<>("name", ColumnType.STRING));
        add(new AbstractMap.SimpleEntry<>("timestamp", ColumnType.DOUBLE));
        add(new AbstractMap.SimpleEntry<>("count", ColumnType.DOUBLE));
        add(new AbstractMap.SimpleEntry<>("max", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("mean", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("min", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("stddev", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("p50", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("p75", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("p95", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("p98", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("p99", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("p999", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("mean_rate", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("m1_rate", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("m5_rate", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("m15_rate", ColumnType.FLOAT));
        add(new AbstractMap.SimpleEntry<>("rate_unit", ColumnType.STRING));
        add(new AbstractMap.SimpleEntry<>("duration_unit", ColumnType.STRING));
    }};

    private final StreamOperationServiceWithoutMetrics streamOperationService;
    private final Clock clock;

    private SiddhiStreamReporter(MetricRegistry registry,
                                 StreamOperationServiceWithoutMetrics streamOperationService,
                                 TimeUnit rateUnit,
                                 TimeUnit durationUnit,
                                 Clock clock,
                                 MetricFilter filter) {
        super(registry, "siddhi-reporter", filter, rateUnit, durationUnit);
        this.streamOperationService = streamOperationService;
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
                TIMER_PROPERTIES,
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
                METER_PROPERTIES,
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
                HISTOGRAM_PROPERTIES,
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
        report(COUNTER_STREAM_NAME, COUNTER_PROPERTIES, name, timestamp, counter.getCount());
    }

    private void reportGauge(long timestamp, String name, Gauge gauge) {
        report(GAUGE_STREAM_NAME, GAUGE_PROPERTIES, name, timestamp, gauge.getValue().toString());
    }


    private void report(String streamName, Set<Map.Entry<String, ColumnType>> properties, Object... values) {
        try {
            List<ColumnNameTypeValue> columns = new ArrayList<>();
            int i = 0;
            for (Map.Entry<String, ColumnType> entry : properties) {
                columns.add(new ColumnNameTypeValue(entry.getKey(), entry.getValue(), values[i]));
                i++;
            }
            streamOperationService.send(streamName, columns);
        } catch (ServiceException e) {
            LOGGER.error("Metric event not sended to stream {}", streamName, e);
        } catch (Exception e) {
            LOGGER.error("FATAL ERROR", e);
        }
    }

    private void createStream(String name, Set<Map.Entry<String, ColumnType>> attributes) {
        List<ColumnNameTypeValue> columns = new ArrayList<>();
        for (Map.Entry<String, ColumnType> attribute : attributes) {
            columns.add(new ColumnNameTypeValue(attribute.getKey(), attribute.getValue(), null));
        }
        streamOperationService.createStream(name, columns);
    }

    public static Builder forRegistry(MetricRegistry registry, StreamOperationServiceWithoutMetrics streamOperationService) {
        return new Builder(registry, streamOperationService);
    }

    public static class Builder {
        private final StreamOperationServiceWithoutMetrics streamOperationService;
        private final MetricRegistry registry;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private Clock clock;
        private MetricFilter filter;

        private Builder(MetricRegistry registry, StreamOperationServiceWithoutMetrics streamOperationService) {
            this.streamOperationService = streamOperationService;
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
            return new SiddhiStreamReporter(registry, streamOperationService,
                    rateUnit,
                    durationUnit,
                    clock,
                    filter);
        }
    }
}

