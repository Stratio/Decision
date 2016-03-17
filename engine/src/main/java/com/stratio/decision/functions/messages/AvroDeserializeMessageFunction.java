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
package com.stratio.decision.functions.messages;

import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.spark.api.java.function.Function;

import com.stratio.decision.commons.avro.InsertMessage;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.serializer.impl.JavaToAvroSerializer;

import scala.Tuple2;

/**
 * Created by josepablofernandez on 14/03/16.
 */
public class AvroDeserializeMessageFunction implements Function<Tuple2<String, byte[]>, StratioStreamingMessage> {

    private static final long serialVersionUID = 5343621517983281408L;

    private JavaToAvroSerializer javaToAvroSerializer = null;

    @Override
    public StratioStreamingMessage call(Tuple2<String, byte[]> message) {

        return deserializeMessage(message._2());

    }

    private StratioStreamingMessage deserializeMessage(byte[] data)  {

        StratioStreamingMessage result = null;
        if (javaToAvroSerializer== null) {
            javaToAvroSerializer = new JavaToAvroSerializer(new SpecificDatumReader(InsertMessage.getClassSchema()));
        }

        return javaToAvroSerializer.deserialize(data);

    }

}
