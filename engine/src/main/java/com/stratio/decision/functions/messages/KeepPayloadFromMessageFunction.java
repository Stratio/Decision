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

import com.google.gson.Gson;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class KeepPayloadFromMessageFunction implements Function<Tuple2<String, String>, StratioStreamingMessage> {

    /**
	 * 
	 */
    private static final long serialVersionUID = 7911766880059394316L;

    public KeepPayloadFromMessageFunction() {

    }

    @Override
    public StratioStreamingMessage call(Tuple2<String, String> message) {

        return buildRequestMessage(message._1(), message._2());
    }

    private StratioStreamingMessage buildRequestMessage(String operation, String requestJson) {

        StratioStreamingMessage requestMessage = null;

        requestMessage = getGson().fromJson(requestJson, StratioStreamingMessage.class);
        requestMessage.setOperation(operation);

        return requestMessage;

    }

    private Gson getGson() {
        return new Gson();
    }

}
