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
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class FilterDataFunction implements Function<Tuple2<StreamAction, Iterable<StratioStreamingMessage>>, Boolean> {

    private final StreamAction actionToFilter;

    public FilterDataFunction(StreamAction actionToFilter) {
        this.actionToFilter = actionToFilter;
    }

    @Override
    public Boolean call(Tuple2<StreamAction, Iterable<StratioStreamingMessage>> tuple) throws Exception {
        if (actionToFilter.equals(tuple._1)) {
            return true;
        } else {
            return false;
        }
    }
}
