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
package com.stratio.streaming.streams;

import org.wso2.siddhi.core.SiddhiManager;

import com.hazelcast.core.IMap;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.utils.SiddhiUtils;

public class StreamSharedStatus {

    private StreamSharedStatus() {

    }

    public static StreamStatusDTO createStreamStatus(String streamName, SiddhiManager siddhiManager) {

        IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance()
                .getMap(STREAMING.STREAM_STATUS_MAP);
        StreamStatusDTO streamStatusDTO = new StreamStatusDTO(streamName, Boolean.TRUE);
        streamStatusDTO.setStreamDefinition(SiddhiUtils.recoverStreamDefinition(siddhiManager
                .getStreamDefinition(streamName)));
        streamStatusMap.put(streamName, streamStatusDTO);
        return streamStatusDTO;

    }

    public static void updateStreamDefinitionStreamStatus(String streamName, SiddhiManager siddhiManager) {

        IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance()
                .getMap(STREAMING.STREAM_STATUS_MAP);
        StreamStatusDTO streamStatusDTO = (StreamStatusDTO) streamStatusMap.get(streamName);

        streamStatusDTO.setStreamDefinition(SiddhiUtils.recoverStreamDefinition(siddhiManager
                .getStreamDefinition(streamName)));
        streamStatusMap.put(streamName, streamStatusDTO);
    }

    public static StreamStatusDTO getStreamStatus(String streamName, SiddhiManager siddhiManager) {

        IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance()
                .getMap(STREAMING.STREAM_STATUS_MAP);

        if (streamStatusMap.get(streamName) != null) {
            return (StreamStatusDTO) streamStatusMap.get(streamName);
        } else {
            // stream status does not exist, this is an special case
            // the stream exists in siddhi becase a previous query has created
            // it
            // so we are going to register it as new
            StreamStatusDTO streamStatusDTO = new StreamStatusDTO(streamName, Boolean.FALSE);
            streamStatusDTO.setStreamDefinition(SiddhiUtils.recoverStreamDefinition(siddhiManager
                    .getStreamDefinition(streamName)));
            streamStatusMap.put(streamName, streamStatusDTO);
            return streamStatusDTO;
        }

    }

    public static void addQueryToStreamStatus(String queryId, String query, String streamName,
            SiddhiManager siddhiManager) {

        IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance()
                .getMap(STREAMING.STREAM_STATUS_MAP);
        StreamStatusDTO streamStatusDTO = (StreamStatusDTO) streamStatusMap.get(streamName);
        streamStatusDTO.getAddedQueries().put(queryId, query);
        streamStatusMap.put(streamStatusDTO.getStreamName(), streamStatusDTO);

    }

    public static String removeQueryInStreamStatus(String queryId, String streamName, SiddhiManager siddhiManager) {

        IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance()
                .getMap(STREAMING.STREAM_STATUS_MAP);
        StreamStatusDTO streamStatusDTO = (StreamStatusDTO) streamStatusMap.get(streamName);
        streamStatusDTO.getAddedQueries().remove(queryId);
        streamStatusMap.put(streamStatusDTO.getStreamName(), streamStatusDTO);
        return queryId;
    }

    public static void changeListenerStreamStatus(Boolean enabled, String streamName, SiddhiManager siddhiManager) {

        IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance()
                .getMap(STREAMING.STREAM_STATUS_MAP);
        StreamStatusDTO streamStatusDTO = (StreamStatusDTO) streamStatusMap.get(streamName);
        if (enabled) {
            streamStatusDTO.enableAction(StreamAction.LISTEN);
        } else {
            streamStatusDTO.disableAction(StreamAction.LISTEN);
        }
        streamStatusMap.put(streamStatusDTO.getStreamName(), streamStatusDTO);

    }

    public static void removeStreamStatus(String streamName, SiddhiManager siddhiManager) {

        IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance()
                .getMap(STREAMING.STREAM_STATUS_MAP);

        streamStatusMap.remove(streamName);

    }

    public static void changeActionStreamStatus(Boolean enable, String streamName, SiddhiManager siddhiManager,
            StreamAction action) {

        IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance()
                .getMap(STREAMING.STREAM_STATUS_MAP);
        StreamStatusDTO streamStatusDTO = (StreamStatusDTO) streamStatusMap.get(streamName);
        if (enable) {
            streamStatusDTO.enableAction(action);
        } else {
            streamStatusDTO.disableAction(action);
        }
        streamStatusMap.put(streamStatusDTO.getStreamName(), streamStatusDTO);

    }
}
