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
package com.stratio.streaming.configuration;

import com.datastax.driver.core.ProtocolOptions;
import com.stratio.streaming.StreamingEngine;
import com.stratio.streaming.commons.constants.BUS;
import com.stratio.streaming.commons.constants.InternalTopic;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.kafka.service.KafkaTopicService;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.*;
import com.stratio.streaming.functions.dal.IndexStreamFunction;
import com.stratio.streaming.functions.dal.ListenStreamFunction;
import com.stratio.streaming.functions.dal.SaveToCassandraStreamFunction;
import com.stratio.streaming.functions.dal.SaveToMongoStreamFunction;
import com.stratio.streaming.functions.ddl.AddQueryToStreamFunction;
import com.stratio.streaming.functions.ddl.AlterStreamFunction;
import com.stratio.streaming.functions.ddl.CreateStreamFunction;
import com.stratio.streaming.functions.dml.InsertIntoStreamFunction;
import com.stratio.streaming.functions.dml.ListStreamsFunction;
import com.stratio.streaming.functions.messages.FilterMessagesByOperationFunction;
import com.stratio.streaming.functions.messages.KeepPayloadFromMessageFunction;
import com.stratio.streaming.serializer.impl.KafkaToJavaSerializer;
import com.stratio.streaming.service.StreamOperationService;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Import(ServiceConfiguration.class)
public class StreamingContextConfiguration {

    private static Logger log = LoggerFactory.getLogger(StreamingContextConfiguration.class);

    @Autowired
    protected ConfigurationContext configurationContext;

    @Autowired
    protected StreamOperationService streamOperationService;

    @Autowired
    private KafkaToJavaSerializer kafkaToJavaSerializer;

    protected KafkaTopicService kafkaTopicService;

    protected JavaStreamingContext create(String streamingContextName, int port, long streamingBatchTime, String sparkHost) {
        SparkConf conf = new SparkConf();
        conf.set("spark.ui.port", String.valueOf(port));
        conf.setAppName(streamingContextName);
        conf.setJars(JavaStreamingContext.jarOfClass(StreamingEngine.class));
        conf.setMaster(sparkHost);

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(streamingBatchTime));

        return streamingContext;
    }

    @PostConstruct
    private void initTopicService() {
        kafkaTopicService = new KafkaTopicService(configurationContext.getZookeeperHostsQuorum(),
                configurationContext.getKafkaConsumerBrokerHost(), configurationContext.getKafkaConsumerBrokerPort(),
                configurationContext.getKafkaConnectionTimeout(), configurationContext.getKafkaSessionTimeout());
    }

}
