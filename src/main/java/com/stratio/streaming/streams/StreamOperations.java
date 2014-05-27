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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.AttributeAlreadyExistException;

import com.google.common.collect.Lists;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.stratio.streaming.callbacks.StreamToBusCallback;
import com.stratio.streaming.callbacks.StreamToCassandraCallback;
import com.stratio.streaming.callbacks.StreamToIndexerCallback;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.commons.messages.StreamQuery;
import com.stratio.streaming.utils.SiddhiUtils;

public class StreamOperations {

    private StreamOperations() {
        // TODO Auto-generated constructor stub
    }

    public static void createStream(StratioStreamingMessage request, SiddhiManager siddhiManager) {
        // create stream in siddhi
        siddhiManager.defineStream(SiddhiUtils.buildDefineStreamSiddhiQL(request));

        // register stream in shared memory
        StreamSharedStatus.createStreamStatus(request.getStreamName(), siddhiManager);
    }

    public static int enlargeStream(StratioStreamingMessage request, SiddhiManager siddhiManager) {

        int addedColumns = 0;
        StreamDefinition streamMetaData = siddhiManager.getStreamDefinition(request.getStreamName());

        for (ColumnNameTypeValue columnNameTypeValue : request.getColumns()) {

            // Siddhi will throw an exception if you try to add a column that
            // already exists,
            // so we first try to find it in the stream
            if (!SiddhiUtils.columnAlreadyExistsInStream(columnNameTypeValue.getColumn(), streamMetaData)) {

                addedColumns++;
                streamMetaData.attribute(columnNameTypeValue.getColumn(),
                        SiddhiUtils.decodeSiddhiType(columnNameTypeValue.getType()));

            } else {
                throw new AttributeAlreadyExistException(columnNameTypeValue.getColumn());
            }
        }

        StreamSharedStatus.updateStreamDefinitionStreamStatus(request.getStreamName(), siddhiManager);

        return addedColumns;

    }

    public static void dropStream(StratioStreamingMessage request, SiddhiManager siddhiManager) {

        // stop all listeners
        siddhiManager.getSiddhiContext().getHazelcastInstance().getTopic(STREAMING.INTERNAL_LISTEN_TOPIC)
                .publish(request.getStreamName());

        // remove all queries
        if (StreamSharedStatus.getStreamStatus(request.getStreamName(), siddhiManager) != null) {
            HashMap<String, String> attachedQueries = StreamSharedStatus.getStreamStatus(request.getStreamName(),
                    siddhiManager).getAddedQueries();

            for (String queryId : attachedQueries.keySet()) {
                siddhiManager.removeQuery(queryId);
            }
        }

        // then we removeStream in siddhi
        siddhiManager.removeStream(request.getStreamName());

        // drop the streamStatus
        StreamSharedStatus.removeStreamStatus(request.getStreamName(), siddhiManager);

    }

    public static void addQueryToExistingStream(StratioStreamingMessage request, SiddhiManager siddhiManager) {
        // add query to siddhi
        String queryId = siddhiManager.addQuery(request.getRequest().replaceAll("timebatch", "timeBatch"));
        // register query in stream status
        StreamSharedStatus
                .addQueryToStreamStatus(queryId, request.getRequest(), request.getStreamName(), siddhiManager);

        // check the streams to see if there are new ones, inferred from queries
        // (not user defined)
        for (StreamDefinition streamMetaData : siddhiManager.getStreamDefinitions()) {
            // by getting the stream, it will be created if don't exists (user
            // defined is false)
            StreamSharedStatus.getStreamStatus(streamMetaData.getStreamId(), siddhiManager);
        }
    }

    public static void removeQueryFromExistingStream(StratioStreamingMessage request, SiddhiManager siddhiManager) {

        // remove query in stream status
        String queryId = StreamSharedStatus.removeQueryInStreamStatus(request.getRequest(), request.getStreamName(),
                siddhiManager);

        // remove query in siddhi
        siddhiManager.removeQuery(queryId);

        // recover all cached streams
        IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance()
                .getMap(STREAMING.STREAM_STATUS_MAP);

        // we will see if siddhi has removed any streams automatically
        for (Entry<Object, Object> streamStatus : streamStatusMap.entrySet()) {

            String streamName = (String) streamStatus.getKey();

            // if this stream does not exist in siddhi
            if (siddhiManager.getStreamDefinition(streamName) == null) {
                // stop all listeners
                siddhiManager.getSiddhiContext().getHazelcastInstance().getTopic(STREAMING.INTERNAL_LISTEN_TOPIC)
                        .publish(streamName);

                // drop the streamStatus
                StreamSharedStatus.removeStreamStatus(streamName, siddhiManager);

            }
        }
    }

    public static List<StratioStreamingMessage> listStreams(StratioStreamingMessage request, SiddhiManager siddhiManager) {

        List<StratioStreamingMessage> streams = Lists.newArrayList();

        for (StreamDefinition streamMetaData : siddhiManager.getStreamDefinitions()) {

            if (!Arrays.asList(STREAMING.STATS_NAMES.STATS_STREAMS).contains(streamMetaData.getStreamId())) {

                List<ColumnNameTypeValue> columns = Lists.newArrayList();
                List<StreamQuery> queries = Lists.newArrayList();
                boolean isUserDefined = false;

                for (Attribute column : streamMetaData.getAttributeList()) {
                    columns.add(new ColumnNameTypeValue(column.getName(),
                            SiddhiUtils.encodeSiddhiType(column.getType()), null));
                }

                if (StreamSharedStatus.getStreamStatus(streamMetaData.getStreamId(), siddhiManager) != null) {
                    HashMap<String, String> attachedQueries = StreamSharedStatus.getStreamStatus(
                            streamMetaData.getStreamId(), siddhiManager).getAddedQueries();

                    for (Entry<String, String> entry : attachedQueries.entrySet()) {
                        queries.add(new StreamQuery(entry.getKey(), entry.getValue()));
                    }

                    isUserDefined = StreamSharedStatus.getStreamStatus(streamMetaData.getStreamId(), siddhiManager)
                            .isUserDefined();
                }

                StratioStreamingMessage streamMessage = new StratioStreamingMessage(streamMetaData.getId(), columns,
                        queries);
                streamMessage.setUserDefined(isUserDefined);

                streams.add(streamMessage);
            }
        }

        return streams;

    }

    public static void listenStream(StratioStreamingMessage request, String kafkaCluster, SiddhiManager siddhiManager) {

        StreamToBusCallback streamCallBack = new StreamToBusCallback(siddhiManager.getStreamDefinition(request
                .getStreamName()), kafkaCluster);

        ITopic<String> listenTopic = siddhiManager.getSiddhiContext().getHazelcastInstance()
                .getTopic(STREAMING.INTERNAL_LISTEN_TOPIC);
        listenTopic.addMessageListener(streamCallBack);

        siddhiManager.addCallback(request.getStreamName(), streamCallBack);

        StreamSharedStatus.changeListenerStreamStatus(Boolean.TRUE, request.getStreamName(), siddhiManager);
    }

    public static void stopListenStream(StratioStreamingMessage request, SiddhiManager siddhiManager) {

        siddhiManager.getSiddhiContext().getHazelcastInstance().getTopic(STREAMING.INTERNAL_LISTEN_TOPIC)
                .publish(request.getStreamName());

        StreamSharedStatus.changeListenerStreamStatus(Boolean.FALSE, request.getStreamName(), siddhiManager);

    }

    public static void save2cassandraStream(StratioStreamingMessage request, String cassandraCluster,
            SiddhiManager siddhiManager) {

        StreamToCassandraCallback cassandraCallBack = new StreamToCassandraCallback(
                siddhiManager.getStreamDefinition(request.getStreamName()), cassandraCluster);

        ITopic<String> save2cassandraTopic = siddhiManager.getSiddhiContext().getHazelcastInstance()
                .getTopic(STREAMING.INTERNAL_SAVE2CASSANDRA_TOPIC);
        save2cassandraTopic.addMessageListener(cassandraCallBack);

        siddhiManager.addCallback(request.getStreamName(), cassandraCallBack);

        StreamSharedStatus.changeSave2CassandraStreamStatus(Boolean.TRUE, request.getStreamName(), siddhiManager);
    }

    public static void stopSave2cassandraStream(StratioStreamingMessage request, SiddhiManager siddhiManager) {

        siddhiManager.getSiddhiContext().getHazelcastInstance().getTopic(STREAMING.INTERNAL_SAVE2CASSANDRA_TOPIC)
                .publish(request.getStreamName());

        StreamSharedStatus.changeSave2CassandraStreamStatus(Boolean.FALSE, request.getStreamName(), siddhiManager);
    }

    public static void streamToIndexer(StratioStreamingMessage request, String elasticSearchHost,
            int elasticSearchPort, SiddhiManager siddhiManager) {

        StreamToIndexerCallback streamToIndexerCallback = new StreamToIndexerCallback(
                siddhiManager.getStreamDefinition(request.getStreamName()), elasticSearchHost, elasticSearchPort);

        ITopic<String> indexerTopic = siddhiManager.getSiddhiContext().getHazelcastInstance()
                .getTopic(STREAMING.INTERNAL_INDEXER_TOPIC);
        indexerTopic.addMessageListener(streamToIndexerCallback);

        siddhiManager.addCallback(request.getStreamName(), streamToIndexerCallback);

        StreamSharedStatus.changeIndexerStreamStatus(Boolean.TRUE, request.getStreamName(), siddhiManager);
    }

    public static void stopStreamToIndexer(StratioStreamingMessage request, SiddhiManager siddhiManager) {

        siddhiManager.getSiddhiContext().getHazelcastInstance().getTopic(STREAMING.INTERNAL_INDEXER_TOPIC)
                .publish(request.getStreamName());
        StreamSharedStatus.changeIndexerStreamStatus(Boolean.FALSE, request.getStreamName(), siddhiManager);
    }
}
