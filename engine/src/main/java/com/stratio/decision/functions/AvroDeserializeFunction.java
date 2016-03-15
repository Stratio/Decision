package com.stratio.decision.functions;

import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.spark.api.java.function.Function;

import com.stratio.decision.commons.avro.InsertMessage;
import com.stratio.decision.commons.messages.StratioStreamingMessage;

import scala.Tuple2;

/**
 * Created by josepablofernandez on 14/03/16.
 */
public class AvroDeserializeFunction implements Function<Tuple2<String, byte[]>, StratioStreamingMessage> {

    private static final long serialVersionUID = 666L;

    // TODO TEST AVRO
    private SpecificDatumReader datumReader = new SpecificDatumReader(InsertMessage.getClassSchema());

    @Override
    public StratioStreamingMessage call(Tuple2<String, byte[]> message) {

        InsertMessage result =  deserializeMessage(message._2());
        return new StratioStreamingMessage();

    }

    private InsertMessage deserializeMessage(byte[] data)  {

        InsertMessage result = null;
        BinaryDecoder bd = DecoderFactory.get().binaryDecoder(data, null);
        try {
            result =  (InsertMessage) datumReader.read(null, bd);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;

    }
}
