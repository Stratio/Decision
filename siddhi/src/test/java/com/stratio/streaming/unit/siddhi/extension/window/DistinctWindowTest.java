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
package com.stratio.streaming.unit.siddhi.extension.window;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import com.stratio.streaming.siddhi.extension.DistinctWindowExtension;

public class DistinctWindowTest {

    private SiddhiManager sm;

    private AtomicInteger count;

    @Before
    public void setUp() {
        count = new AtomicInteger(0);
        initSiddhi();
    }

    @Test
    public void createQueryTest() throws InterruptedException {
        sm.defineStream("define stream testStream (c1 string, c2 float, c3 int);");
        sm.addQuery("from testStream #window.stratio:distinct(c1) select c1, c2,c3 insert into resultStream;");
        assertNotNull(sm.getStreamDefinition("resultStream"));
    }

    @Test
    public void distinctFilterTest() throws InterruptedException {
        sm.defineStream("define stream testStream (c1 string, c2 float, c3 int);");
        sm.addQuery("from testStream #window.stratio:distinct(c1) select c1, c2,c3 insert into resultStream;");

        sm.addCallback("resultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    if (event instanceof InEvent) {
                        count.getAndIncrement();
                    }
                }
            }
        });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(10), new Integer(20) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(20), new Integer(30) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_B"), new Float(30), new Integer(40) });

        Thread.sleep(500);
        assertEquals(2, count.get());
    }

    @Test
    public void distinctFilterTestRepeatedObjectsTest() throws InterruptedException {
        sm.defineStream("define stream testStream (c1 string, c2 float, c3 int);");
        sm.addQuery("from testStream #window.stratio:distinct(c1) select c1, c2,c3 insert into resultStream;");

        sm.addCallback("resultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    if (event instanceof InEvent) {
                        count.getAndIncrement();
                    }
                }
            }
        });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(10), new Integer(20) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(20), new Integer(30) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_B"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(10), new Integer(20) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(20), new Integer(30) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_B"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(10), new Integer(20) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(20), new Integer(30) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(10), new Integer(20) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(20), new Integer(30) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_B"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_B"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(10), new Integer(20) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(20), new Integer(30) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_B"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(10), new Integer(20) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(20), new Integer(30) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_B"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(10), new Integer(20) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(20), new Integer(30) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_B"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(10), new Integer(20) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(10), new Integer(20) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(20), new Integer(30) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_A"), new Float(30), new Integer(40) });
        sm.getInputHandler("testStream").send(new Object[] { new String("KEY_B"), new Float(30), new Integer(40) });

        Thread.sleep(500);
        assertEquals(16, count.get());
    }

    private void initSiddhi() {
        SiddhiConfiguration config = new SiddhiConfiguration();
        @SuppressWarnings("rawtypes")
        List<Class> extensions = new ArrayList<>();
        extensions.add(DistinctWindowExtension.class);
        config.setSiddhiExtensions(extensions);
        sm = new SiddhiManager(config);
    }
}
