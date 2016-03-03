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
package com.stratio.decision.commons.constants;

/**
 * Created by josepablofernandez on 14/01/16.
 */
public interface ENGINE_ACTIONS_PARAMETERS {

    public interface DROOLS {

        public static final String GROUP = "groupName";
        public static final String CEP_OUTPUT_STREAM = "outputStream";
        public static final String KAFKA_OUTPUT_TOPIC = "kafkaTopic";
    }
}
