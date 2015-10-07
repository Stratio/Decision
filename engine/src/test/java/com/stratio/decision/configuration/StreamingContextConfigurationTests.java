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


