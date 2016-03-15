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
package com.stratio.decision.functions;

import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.spark.api.java.function.Function;

import com.stratio.decision.commons.avro.InsertMessage;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;

import scala.Tuple2;

/**
 * Created by josepablofernandez on 14/03/16.
 */
public class AvroDeserializeFunction implements Function<Tuple2<String, byte[]>, StratioStreamingMessage> {

    private static final long serialVersionUID = 666L;

    private SpecificDatumReader datumReader =  null;

    @Override
    public StratioStreamingMessage call(Tuple2<String, byte[]> message) {

        return deserializeMessage(message._2());

    }


    private StratioStreamingMessage deserializeMessage(byte[] data)  {

        StratioStreamingMessage result = null;
        BinaryDecoder bd = DecoderFactory.get().binaryDecoder(data, null);

        if (datumReader == null) {
            datumReader =  new SpecificDatumReader(InsertMessage.getClassSchema());
        }
        try {
            InsertMessage insertMessage =  (InsertMessage) datumReader.read(null, bd);
            result = convertMessage(insertMessage);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;

    }

    private StratioStreamingMessage convertMessage(InsertMessage insertMessage){

        StratioStreamingMessage stratioStreamingMessage =  new StratioStreamingMessage();
        stratioStreamingMessage.setOperation(insertMessage.getOperation().toString());
        stratioStreamingMessage.setStreamName(insertMessage.getStreamName().toString());
        stratioStreamingMessage.setSession_id(insertMessage.getSessionId().toString());


        insertMessage.getData().forEach(

                data -> {

                    ColumnNameTypeValue columnNameTypeValue = new ColumnNameTypeValue();
                    columnNameTypeValue.setColumn(data.getColumn().toString());

                    switch (data.getType().toString()) {
                        case "java.lang.Double" : Double doubleData = new Double(data.getValue().toString());
                                                  columnNameTypeValue.setValue(doubleData);
                                                  break;
                        case "java.lang.Float" : Float floatData = new Float(data.getValue().toString());
                                                 columnNameTypeValue.setValue(floatData);
                                                 break;
                        case "java.lang.Integer" : Integer integerData = new Integer(data.getValue().toString());
                                                   columnNameTypeValue.setValue(integerData);
                                                   break;
                        case "java.lang.Long" : Long longData = new Long(data.getValue().toString());
                                                columnNameTypeValue.setValue(longData);
                                                break;
                        case "java.lang.Boolean" : Boolean booleanData = new Boolean(data.getValue().toString());
                                                   columnNameTypeValue.setValue(booleanData);
                                                   break;
                        default: columnNameTypeValue.setValue(data.getValue().toString());
                    }

                    stratioStreamingMessage.addColumn(columnNameTypeValue);
        }
        );


        return  stratioStreamingMessage;

    }
}
