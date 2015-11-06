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

public interface STREAMING {
    String ZK_BASE_PATH = "/stratio/streaming";
    String STREAM_STATUS_MAP = "stratio_stream_map";

    String ZK_EPHEMERAL_NODE_PATH = "/stratio/streaming/engine";
    String ZK_EPHEMERAL_NODE_STATUS_PATH = "/stratio/streaming/status";
    String ZK_EPHEMERAL_NODE_STATUS_CONNECTED = "connected";
    String ZK_EPHEMERAL_NODE_STATUS_INITIALIZED = "initialized";
    String STREAMING_KEYSPACE_NAME = "stratio_streaming";
    String ZK_PERSISTENCE_NODE = "/failoverStorage";
    String ZK_PERSISTENCE_STORE_PATH = ZK_BASE_PATH + ZK_PERSISTENCE_NODE;
    String ZK_HIGH_AVAILABILITY_NODE = "/latch";
    String ZK_HIGH_AVAILABILITY_PATH = ZK_BASE_PATH + ZK_HIGH_AVAILABILITY_NODE;

    public interface STATS_NAMES {
        String SINK_STREAM_PREFIX = "VOID_";
        String BASE = "stratio_stats_base";
        String GLOBAL_STATS_BY_OPERATION = "stratio_stats_global_by_operation";
        String[] STATS_STREAMS = new String[] { BASE, GLOBAL_STATS_BY_OPERATION };

    }

}
