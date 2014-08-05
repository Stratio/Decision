package com.stratio.streaming.serializer;

import java.io.Serializable;
import java.util.List;

public interface Serializer<A, B> extends Serializable {

    public B serialize(A object);

    public A deserialize(B object);

    public List<B> serialize(List<A> object);

    public List<A> deserialize(List<B> object);

}
