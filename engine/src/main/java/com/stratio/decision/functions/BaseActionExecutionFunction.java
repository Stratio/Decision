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

import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;



public abstract class BaseActionExecutionFunction implements
        Function<JavaPairRDD<StreamAction, Iterable<StratioStreamingMessage>>, Void> {

    private static final long serialVersionUID = -7719763983201600088L;

    static final String TIMESTAMP_FIELD = "timestamp";

    @Override
    public Void call(JavaPairRDD<StreamAction, Iterable<StratioStreamingMessage>> rdd) throws Exception {

//        List<Tuple2<StreamAction, Iterable<StratioStreamingMessage>>> rddContent = rdd.collect();
//        if (rddContent.size() != 0) {
//            process(rddContent.get(0)._2());
//        }


        rdd.mapPartitions(
                new FlatMapFunction<Iterator<Tuple2<StreamAction,Iterable<StratioStreamingMessage>>>, Object>() {

                    @Override public Iterable<Object> call(
                            Iterator<Tuple2<StreamAction, Iterable<StratioStreamingMessage>>> tuple2Iterator)
                            throws Exception {


                        while (tuple2Iterator.hasNext()){
                                process(tuple2Iterator.next()._2());
                        }

                        return new ArrayList<Object>();
                    }
                }).count();



//        rdd.foreach(new VoidFunction<Tuple2<StreamAction, Iterable<StratioStreamingMessage>>>() {
//            @Override public void call(Tuple2<StreamAction, Iterable<StratioStreamingMessage>> streamActionIterableTuple2)
//                    throws Exception {
//                process(streamActionIterableTuple2._2());
//            }
//        });


        return null;
    }

    public abstract void process(Iterable<StratioStreamingMessage> messages) throws Exception;

    public abstract Boolean check() throws Exception;
}
