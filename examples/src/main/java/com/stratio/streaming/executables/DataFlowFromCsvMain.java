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
package com.stratio.streaming.executables;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.stratio.streaming.commons.constants.InternalTopic;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

public class DataFlowFromCsvMain {

    private static final Logger log = LoggerFactory.getLogger(DataFlowFromCsvMain.class);

    public static void main(String[] args) throws IOException, NumberFormatException, InterruptedException {
        if (args.length < 4) {
            log.info("Usage: \n param 1 - path to file \n param 2 - stream name to send the data \n param 3 - time in ms to wait to send each data \n param 4 - broker list");
        } else {
            Producer<String, String> producer = new Producer<String, String>(createProducerConfig(args[3]));
            Gson gson = new Gson();

            Reader in = new FileReader(args[0]);
            CSVParser parser = CSVFormat.DEFAULT.parse(in);

            List<String> columnNames = new ArrayList<>();
            for (CSVRecord csvRecord : parser.getRecords()) {

                if (columnNames.size() == 0) {
                    Iterator<String> iterator = csvRecord.iterator();
                    while (iterator.hasNext()) {
                        columnNames.add(iterator.next());
                    }
                } else {
                    StratioStreamingMessage message = new StratioStreamingMessage();

                    message.setOperation(STREAM_OPERATIONS.MANIPULATION.INSERT.toLowerCase());
                    message.setStreamName(args[1]);
                    message.setTimestamp(System.currentTimeMillis());
                    message.setSession_id(String.valueOf(System.currentTimeMillis()));
                    message.setRequest_id(String.valueOf(System.currentTimeMillis()));
                    message.setRequest("dummy request");

                    List<ColumnNameTypeValue> sensorData = new ArrayList<>();
                    for (int i = 0; i < columnNames.size(); i++) {

                        // Workaround
                        Object value = null;
                        try {
                            value = Double.valueOf(csvRecord.get(i));
                        } catch (NumberFormatException e) {
                            value = csvRecord.get(i);
                        }
                        sensorData.add(new ColumnNameTypeValue(columnNames.get(i), null, value));
                    }

                    message.setColumns(sensorData);

                    String json = gson.toJson(message);
                    log.info("Sending data: {}", json);
                    producer.send(new KeyedMessage<String, String>(InternalTopic.TOPIC_DATA.getTopicName(),
                            STREAM_OPERATIONS.MANIPULATION.INSERT, json));

                    log.info("Sleeping {} ms...", args[2]);
                    Thread.sleep(Long.valueOf(args[2]));
                }
            }
            log.info("Program completed.");
        }
    }

    private static ProducerConfig createProducerConfig(String brokerList) {
        Properties properties = new Properties();
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list", brokerList);
        return new ProducerConfig(properties);
    }
}
