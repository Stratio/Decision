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
package com.stratio.decision.executables;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.stratio.decision.commons.avro.Action;
import com.stratio.decision.commons.avro.ColumnType;
import com.stratio.decision.commons.avro.InsertMessage;
import com.stratio.decision.commons.constants.InternalTopic;
import com.stratio.decision.commons.constants.STREAM_OPERATIONS;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.generator.ValuesGenerator;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

public class HardwareEmulatorMain {

    public static final String sensorDataStream = "sensor_grid";

    public static void main(String[] args) {

        int multiplicator = 1;
        KafkaProducer<String, byte[]> avroProducer = null;

        if (args != null && args.length > 1) {
            multiplicator = Integer.valueOf(args[0]);
            avroProducer = new KafkaProducer<String, byte[]>(createKafkaProducerConfig(args[1]));
        } else {
            throw new RuntimeException(
                    "Usage: \n param 1 - data multiplier (default = 1) \n param 2 - kafka broker list");
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
        es.execute(new DataSender(avroProducer, cpuValues, "cpu"));
        es.execute(new DataSender(avroProducer, memValues, "memory"));
        es.execute(new DataSender(avroProducer, diskUsageValues, "disk usage"));
        es.execute(new DataSender(avroProducer, memorySwapValues, "memory swap"));
        es.execute(new DataSender(avroProducer, threadsValues, "threads"));
        es.execute(new DataSender(avroProducer, runningValues, "running processes"));

        es.shutdown();
    }

    private static Properties createKafkaProducerConfig(String brokerList) {
        Properties properties = new Properties();


        properties.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        properties.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        return  properties;

    }

    private static class DataSender implements Runnable {

        private final KafkaProducer<String, byte[]> avroProducer;
        private final List<Double> values;
        private final String name;

        public DataSender(KafkaProducer<String, byte[]> avroProducer, List<Double> values, String name) {
            super();
            this.avroProducer = avroProducer;
            this.values = values;
            this.name = name;
        }

        @Override
        public void run() {

            for (StratioStreamingMessage message : generateStratioStreamingMessages(values, name)) {

                ProducerRecord busMessage = new ProducerRecord(InternalTopic.TOPIC_DATA.getTopicName(), message.getOperation(),
                        serializeStratioStreamingMessage(message));
                avroProducer.send(busMessage);

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
                sensorData.add(new ColumnNameTypeValue("data", null, value));

                message.setColumns(sensorData);

                result.add(message);
            }
            return result;
        }

        private byte[] serializeStratioStreamingMessage(StratioStreamingMessage message){

            List<com.stratio.decision.commons.avro.ColumnType> columns = null;

            if (message.getColumns() != null) {
                columns = new java.util.ArrayList<>();
                ColumnType c = null;

                for (ColumnNameTypeValue messageColumn : message.getColumns()) {
                    c = new ColumnType(messageColumn.getColumn(), messageColumn.getValue().toString(),
                            messageColumn.getValue()
                                    .getClass().getName());
                    columns.add(c);
                }

            }

            InsertMessage insertMessage =  new InsertMessage(message.getOperation(), message.getStreamName(), message
                    .getSession_id(), message.getTimestamp(), columns, null);

            return getInsertMessageBytes(insertMessage);

        }

        private byte[] getInsertMessageBytes(InsertMessage insertMessage){

            byte[] result = null;

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            SpecificDatumWriter writer = new SpecificDatumWriter<InsertMessage>(InsertMessage.getClassSchema());

            try {
                writer.write(insertMessage, encoder);
                encoder.flush();
                out.close();
                result = out.toByteArray();
            }catch (IOException e){
                return null;
            }

            return result;

        }

    }
}
