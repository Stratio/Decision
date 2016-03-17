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

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/**
 * Created by josepablofernandez on 16/03/16.
 */
public class FilterAvroMessagesByOperationFunction implements Function<Tuple2<String, byte[]>, Boolean> {

    private static final long serialVersionUID = 8595035558487818620L;

    private String allowedOperation;

    public FilterAvroMessagesByOperationFunction(String operation) {
        this.allowedOperation = operation;
    }

    @Override
    public Boolean call(Tuple2<String, byte[]> message) throws Exception {
        if (message._1() != null && message._1().equalsIgnoreCase(allowedOperation)) {
            return true;
        }
        return false;
    }
}
