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
package com.stratio.decision.configuration;

import static org.junit.Assert.assertEquals;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.stratio.decision.configuration.StreamingContextConfiguration;

/**
 * Created by eruiz on 5/10/15.
 */
public class StreamingContextConfigurationTests {





        private static SparkConf conf;
        private static JavaStreamingContext ssc;
        private static JavaStreamingContext sc;

        private StreamingContextConfiguration streamingContextConfiguration = Mockito.mock(StreamingContextConfiguration
                .class);

        @Before
        public void setUp() throws Exception {
            System.clearProperty("spark.driver,port");
            System.clearProperty("spark.hostPort");

            conf = new SparkConf().setMaster("local[4]").setAppName("magic");
            ssc = new JavaStreamingContext(conf, Durations.seconds(1));
            //        TODO simulate a configurationContext
            sc = streamingContextConfiguration.streamingContext();
            //sc.start();
            //        sc.ssc().conf();
            //        sc.start();
            //        ssc.start();

        }

        @After
        public void tearDown() throws Exception {
            try {
                if (ssc instanceof JavaStreamingContext) {
                    ssc.stop();
                    sc.stop();
                }
            } catch (Exception ex) {
            }
        }

        @Test
        public void testActionBaseFunctionCall() throws Exception {
            //        sc.sparkContext().emptyRDD().rdd().first();
            //        ssc.sparkContext().emptyRDD().rdd().first();
            assertEquals(sc instanceof JavaStreamingContext, false);
            assertEquals(ssc.sparkContext().appName(), "magic");

        }

    }


