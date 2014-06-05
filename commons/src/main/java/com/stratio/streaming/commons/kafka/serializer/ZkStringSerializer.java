package com.stratio.streaming.commons.kafka.serializer;

import kafka.utils.ZKStringSerializer;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

/**
 * A simple zookeeper serializer implementation
 * 
 * @author ajnavarro
 * 
 */
public class ZkStringSerializer implements ZkSerializer {

    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
        return ZKStringSerializer.serialize(data);
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return ZKStringSerializer.deserialize(bytes);
    }

}
