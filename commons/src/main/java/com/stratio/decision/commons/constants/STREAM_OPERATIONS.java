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

import java.util.ArrayList;
import java.util.List;

public interface STREAM_OPERATIONS {
    // DDL
    public interface DEFINITION {
        String CREATE = "CREATE";
        String ADD_QUERY = "ADD_QUERY";
        String REMOVE_QUERY = "REMOVE_QUERY";
        String DROP = "DROP";
        String ALTER = "ALTER";
    }

    // DML
    public interface MANIPULATION {
        String INSERT = "INSERT";
        String LIST = "LIST";
    }

    // DAL
    public interface ACTION {
        String LISTEN = "LISTEN";
        String STOP_LISTEN = "STOP_LISTEN";
        String SAVETO_SOLR = "SAVETO_SOLR";
        String STOP_SAVETO_SOLR = "STOP_SAVETO_SOLR";
        String SAVETO_CASSANDRA = "SAVETO_CASSANDRA";
        String STOP_SAVETO_CASSANDRA = "STOP_SAVETO_CASSANDRA";
        String SAVETO_MONGO = "SAVETO_MONGO";
        String STOP_SAVETO_MONGO = "STOP_SAVETO_MONGO";
        String INDEX = "INDEX";
        String STOP_INDEX = "STOP_INDEX";
        String START_SENDTODROOLS = "START_SENDTO_DROOLS";
        String STOP_SENDTODROOLS = "STOP_SENDTO_DROOLS";
    }

    public class SyncOperations {

        public static List<String> getAckOperations() {

            List<String> operations = new ArrayList<>();

            operations.add(DEFINITION.CREATE.toLowerCase());
            operations.add(DEFINITION.ADD_QUERY.toLowerCase());
            operations.add(DEFINITION.REMOVE_QUERY.toLowerCase());
            operations.add(DEFINITION.DROP.toLowerCase());
            operations.add(DEFINITION.ALTER.toLowerCase());

            operations.add(MANIPULATION.LIST.toLowerCase());

            operations.add(ACTION.LISTEN.toLowerCase());
            operations.add(ACTION.STOP_LISTEN.toLowerCase());
            operations.add(ACTION.SAVETO_SOLR.toLowerCase());
            operations.add(ACTION.STOP_SAVETO_SOLR.toLowerCase());
            operations.add(ACTION.SAVETO_CASSANDRA.toLowerCase());
            operations.add(ACTION.STOP_SAVETO_CASSANDRA.toLowerCase());
            operations.add(ACTION.SAVETO_MONGO.toLowerCase());
            operations.add(ACTION.STOP_SAVETO_MONGO.toLowerCase());
            operations.add(ACTION.INDEX.toLowerCase());
            operations.add(ACTION.STOP_INDEX.toLowerCase());

            return operations;
        }
    }

}
