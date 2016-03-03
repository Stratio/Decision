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
package com.stratio.decision.metrics;

import com.codahale.metrics.*;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.stratio.decision.configuration.ServiceConfiguration;
import com.stratio.decision.configuration.StreamingSiddhiConfiguration;
import com.stratio.decision.dao.StreamStatusDao;
import com.stratio.decision.service.StreamOperationServiceWithoutMetrics;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNull;

/**
 * Created by aitor on 9/25/15.
 */
public class SiddhiStreamReporterTest {

    private SiddhiStreamReporter.Builder builder;

    private SiddhiStreamReporter reporter;

    private StreamOperationServiceWithoutMetrics streamOperationService;

    @Before
    public void setUp() throws Exception {
        SiddhiManager siddhiManager= new StreamingSiddhiConfiguration().siddhiManager();
        ServiceConfiguration serviceConfiguration= new ServiceConfiguration();

        streamOperationService= new StreamOperationServiceWithoutMetrics(
                siddhiManager, new StreamStatusDao(), serviceConfiguration.callbackService());

        builder= SiddhiStreamReporter.forRegistry(new MetricRegistry(), streamOperationService);
    }

    @Test
    public void testReport() throws Exception {
        Clock clock= Clock.defaultClock();

        reporter= builder.convertRatesTo(TimeUnit.MINUTES)
                .convertDurationsTo(TimeUnit.SECONDS)
                .withClock(clock)
                .filter(MetricFilter.ALL)
                .build();

        reporter.start(1, TimeUnit.SECONDS);

        SortedMap<String, Gauge> gauges= new TreeMap<>();
        SortedMap<String, Counter> counters= new TreeMap<>();
        SortedMap<String, Histogram> histograms= new TreeMap<>();
        SortedMap<String, Meter> meters= new TreeMap<>();
        SortedMap<String, Timer> timers= new TreeMap<>();

        Gauge gauge= new FileDescriptorRatioGauge();
        gauges.put("gauges", gauge);

        Counter counter= new Counter();
        counters.put("counters", counter);

        Meter meter= new Meter();
        meters.put("meters", meter);

        Timer timer= new Timer();
        timers.put("timers", timer);

        Exception ex= null;
        try {
            reporter.report(gauges, counters, histograms, meters, timers);
        } catch (Exception e)   {ex= e; }
        assertNull("Expected null value that means not exception", ex);

    }

    @Test
    public void testForRegistry() throws Exception {

    }
}