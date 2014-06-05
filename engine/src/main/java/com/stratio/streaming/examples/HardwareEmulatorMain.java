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
package com.stratio.streaming.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.stratio.streaming.commons.constants.BUS;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

public class HardwareEmulatorMain {

    public static final String sensorDataStream = "sensor_grid";

    public static void main(String[] args) {

        int multiplicator = 1;
        Producer<String, String> producer = null;
        if (args != null && args.length > 1) {
            multiplicator = Integer.valueOf(args[0]);
            producer = new Producer<String, String>(createProducerConfig(args[1]));
        } else {
            throw new RuntimeException("Parameters are incorrect!");
        }

        System.out.println("CPU VALUES");
        List<Double> cpuValues = new ValuesGenerator(10).withDerivation(5).addRange(50, 1000 * multiplicator)
                .addRange(50, 100 * multiplicator).addRange(101, 100 * multiplicator)
                .addRange(100, 1000 * multiplicator).addRange(30, 1000 * multiplicator)
                .addRange(20, 100 * multiplicator).addRange(20, 100 * multiplicator).build();

        System.out.println("MEM VALUES");
        List<Double> memValues = new ValuesGenerator(100).withDerivation(3).addRange(50, 1000 * multiplicator)
                .addRange(92, 100 * multiplicator).addRange(50, 100 * multiplicator).addRange(92, 1000 * multiplicator)
                .addRange(100, 1000 * multiplicator).addRange(40, 1000 * multiplicator)
                .addRange(40, 1000 * multiplicator).addRange(40, 1000 * multiplicator).build();

        System.out.println("DISK VALUES");
        List<Double> diskUsageValues = new ValuesGenerator(40).withDerivation(10).addRange(50, 1000 * multiplicator)
                .addRange(15, 100 * multiplicator).addRange(50, 100 * multiplicator).addRange(15, 1000 * multiplicator)
                .addRange(100, 1000 * multiplicator).addRange(40, 1000 * multiplicator)
                .addRange(40, 100 * multiplicator).addRange(40, 1000 * multiplicator).build();

        System.out.println("SWAP VALUES");
        List<Double> memorySwapValues = new ValuesGenerator(0).withDerivation(10).addRange(0, 1000 * multiplicator)
                .addRange(2, 100 * multiplicator).addRange(10, 100 * multiplicator).addRange(15, 1000 * multiplicator)
                .addRange(100, 1000 * multiplicator).addRange(0, 1000 * multiplicator).addRange(4, 100 * multiplicator)
                .addRange(4, 1000 * multiplicator).build();

        System.out.println("THREADS VALUES");
        List<Double> threadsValues = new ValuesGenerator(0).withDerivation(10).addRange(3, 1000 * multiplicator)
                .addRange(20, 100 * multiplicator).addRange(10, 100 * multiplicator).addRange(5, 1000 * multiplicator)
                .addRange(1, 1000 * multiplicator).addRange(8, 1000 * multiplicator).addRange(40, 100 * multiplicator)
                .addRange(42, 1000 * multiplicator).build();

        System.out.println("RUNNING VALUES");
        List<Double> runningValues = new ValuesGenerator(0).withDerivation(10).addRange(3, 1000 * multiplicator)
                .addRange(20, 100 * multiplicator).addRange(10, 100 * multiplicator).addRange(5, 1000 * multiplicator)
                .addRange(1, 1000 * multiplicator).addRange(8, 1000 * multiplicator).addRange(40, 100 * multiplicator)
                .addRange(42, 1000 * multiplicator).build();

        int totalValues = cpuValues.size() + memValues.size() + diskUsageValues.size() + memorySwapValues.size()
                + threadsValues.size() + runningValues.size();

        System.out.println("TOTAL VALUES: " + totalValues);

        ExecutorService es = Executors.newFixedThreadPool(10);
        es.execute(new DataSender(producer, cpuValues, "cpu"));
        es.execute(new DataSender(producer, memValues, "memory"));
        es.execute(new DataSender(producer, diskUsageValues, "disk usage"));
        es.execute(new DataSender(producer, memorySwapValues, "memory swap"));
        es.execute(new DataSender(producer, threadsValues, "threads"));
        es.execute(new DataSender(producer, runningValues, "running processes"));

        es.shutdown();
    }

    private static ProducerConfig createProducerConfig(String brokerList) {
        Properties properties = new Properties();
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list", brokerList);
        return new ProducerConfig(properties);
    }

    private static class DataSender implements Runnable {

        private final Producer<String, String> producer;
        private final List<Double> values;
        private final String name;

        public DataSender(Producer<String, String> producer, List<Double> values, String name) {
            super();
            this.producer = producer;
            this.values = values;
            this.name = name;
        }

        @Override
        public void run() {
            Gson gson = new Gson();

            for (StratioStreamingMessage message : generateStratioStreamingMessages(values, name)) {
                KeyedMessage<String, String> busMessage = new KeyedMessage<String, String>(BUS.TOPICS,
                        STREAM_OPERATIONS.MANIPULATION.INSERT, gson.toJson(message));
                producer.send(busMessage);
            }

        }

        private List<StratioStreamingMessage> generateStratioStreamingMessages(List<Double> values, String name) {
            List<StratioStreamingMessage> result = new ArrayList<StratioStreamingMessage>();

            for (Double value : values) {
                StratioStreamingMessage message = new StratioStreamingMessage();

                message.setOperation(STREAM_OPERATIONS.MANIPULATION.INSERT);
                message.setStreamName(sensorDataStream);
                message.setTimestamp(System.currentTimeMillis());
                message.setSession_id(String.valueOf(System.currentTimeMillis()));
                message.setRequest_id(String.valueOf(System.currentTimeMillis()));
                message.setRequest("dummy request");

                List<ColumnNameTypeValue> sensorData = Lists.newArrayList();
                sensorData.add(new ColumnNameTypeValue("name", null, name));
                sensorData.add(new ColumnNameTypeValue("ind", null, 1));
                sensorData.add(new ColumnNameTypeValue("data", null, value));

                message.setColumns(sensorData);

                result.add(message);
            }
            return result;
        }

    }
}
