/*******************************************************************************
 * Copyright 2014 Stratio
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.stratio.streaming.commons.constants;

public interface STREAM_OPERATIONS {
    // DDL
    public interface DEFINITION {
        public static final String CREATE = "CREATE";
        public static final String ADD_QUERY = "ADD_QUERY";
        public static final String REMOVE_QUERY = "REMOVE_QUERY";
        public static final String DROP = "DROP";
        public static final String ALTER = "ALTER";
    }

    // DML
    public interface MANIPULATION {
        public static final String INSERT = "INSERT";
        public static final String LIST = "LIST";
    }

    // DAL
    public interface ACTION {
        public static final String LISTEN = "LISTEN";
        public static final String STOP_LISTEN = "STOP_LISTEN";
        public static final String SAVETO_CASSANDRA = "SAVETO_CASSANDRA";
        public static final String INDEX = "INDEX";
    }

}
