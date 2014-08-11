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
package com.stratio.streaming.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.QueryFactory;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import com.codahale.metrics.annotation.Timed;
import com.ryantenney.metrics.annotation.Counted;
import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.commons.messages.StreamQuery;
import com.stratio.streaming.dao.StreamStatusDao;
import com.stratio.streaming.exception.ServiceException;
import com.stratio.streaming.streams.QueryDTO;
import com.stratio.streaming.streams.StreamStatusDTO;
import com.stratio.streaming.utils.SiddhiUtils;

public class StreamOperationService {

    private final SiddhiManager siddhiManager;

    private final StreamStatusDao streamStatusDao;

    private final CallbackService callbackService;

    public StreamOperationService(SiddhiManager siddhiManager, StreamStatusDao streamStatusDao,
            CallbackService callbackService) {
        this.siddhiManager = siddhiManager;
        this.streamStatusDao = streamStatusDao;
        this.callbackService = callbackService;
    }

    @Counted(absolute = true, name = "streams.total.created", monotonic = true)
    public void createStream(String streamName, List<ColumnNameTypeValue> columns) {
        StreamDefinition newStream = QueryFactory.createStreamDefinition().name(streamName);
        for (ColumnNameTypeValue column : columns) {
            newStream.attribute(column.getColumn(), getSiddhiType(column.getType()));
        }
        siddhiManager.defineStream(newStream);
        streamStatusDao.create(streamName);
    }

    public boolean streamExist(String streamName) {
        return streamStatusDao.get(streamName) != null ? true : false;
    }

    public boolean isUserDefined(String streamName) {
        StreamStatusDTO streamStatus = streamStatusDao.get(streamName);
        return streamStatus != null ? streamStatus.getUserDefined() : false;
    }

    @Counted(absolute = true, name = "streams.total.altered", monotonic = true)
    public int enlargeStream(String streamName, List<ColumnNameTypeValue> columns) throws ServiceException {
        int addedColumns = 0;
        StreamDefinition streamMetaData = siddhiManager.getStreamDefinition(streamName);
        for (ColumnNameTypeValue columnNameTypeValue : columns) {
            if (!SiddhiUtils.columnAlreadyExistsInStream(columnNameTypeValue.getColumn(), streamMetaData)) {
                addedColumns++;
                streamMetaData.attribute(columnNameTypeValue.getColumn(), getSiddhiType(columnNameTypeValue.getType()));
            } else {
                throw new ServiceException(String.format("Alter stream error, Column %s already exists.",
                        columnNameTypeValue.getColumn()));
            }
        }

        return addedColumns;
    }

    @Counted(absolute = true, name = "streams.total.deleted", monotonic = true)
    public void dropStream(String streamName) {

        Map<String, QueryDTO> attachedQueries = streamStatusDao.get(streamName).getAddedQueries();
        for (String queryId : attachedQueries.keySet()) {
            siddhiManager.removeQuery(queryId);
        }
        siddhiManager.removeStream(streamName);
        streamStatusDao.remove(streamName);
    }

    @Counted(absolute = true, name = "queries.total.added", monotonic = true)
    public void addQuery(String streamName, String queryString) {
        String queryId = siddhiManager.addQuery(queryString);
        streamStatusDao.addQuery(streamName, queryId, queryString);
        for (StreamDefinition streamDefinition : siddhiManager.getStreamDefinitions()) {
            // TODO refactor to obtain exactly siddhi inferred streams.
            streamStatusDao.createInferredStream(streamDefinition.getStreamId());
        }
    }

    @Counted(absolute = true, name = "queries.total.removed", monotonic = true)
    public void removeQuery(String queryId, String streamName) {
        siddhiManager.removeQuery(queryId);
        streamStatusDao.removeQuery(streamName, queryId);
        for (Entry<String, StreamStatusDTO> streamStatus : streamStatusDao.getAll().entrySet()) {
            String temporalStreamName = streamStatus.getKey();
            if (siddhiManager.getStreamDefinition(temporalStreamName) == null) {
                this.dropStream(temporalStreamName);
            }
        }
    }

    public boolean queryIdExists(String streamName, String queryId) {
        StreamStatusDTO streamStatus = streamStatusDao.get(streamName);
        if (streamStatus != null) {
            return streamStatus.getAddedQueries().containsKey(queryId);
        } else {
            return false;
        }
    }

    public boolean queryRawExists(String streamName, String queryRaw) {
        StreamStatusDTO streamStatus = streamStatusDao.get(streamName);
        if (streamStatus != null) {
            return streamStatus.getAddedQueries().containsValue(new QueryDTO(queryRaw));
        } else {
            return false;
        }
    }

    public void enableAction(String streamName, StreamAction action) {

        if (streamStatusDao.getEnabledActions(streamName).size() == 0) {
            String actionQueryId = siddhiManager.addQuery(QueryFactory.createQuery()
                    .from(QueryFactory.inputStream(streamName))
                    .insertInto(STREAMING.STATS_NAMES.SINK_STREAM_PREFIX.concat(streamName)));

            streamStatusDao.setActionQuery(streamName, actionQueryId);

            siddhiManager.addCallback(actionQueryId,
                    callbackService.add(streamName, streamStatusDao.getEnabledActions(streamName)));
        }

        streamStatusDao.enableAction(streamName, action);
    }

    public void disableAction(String streamName, StreamAction action) {
        streamStatusDao.disableAction(streamName, action);

        if (streamStatusDao.getEnabledActions(streamName).size() == 0) {
            String actionQueryId = streamStatusDao.getActionQuery(streamName);
            if (actionQueryId != null) {
                siddhiManager.removeQuery(actionQueryId);
            }
            callbackService.remove(streamName);
        }
    }

    public boolean isActionEnabled(String streamName, StreamAction action) {
        return streamStatusDao.getEnabledActions(streamName).contains(action);
    }

    @Counted(absolute = true, name = "streams.total.listed", monotonic = true)
    public List<StratioStreamingMessage> list() {
        List<StratioStreamingMessage> result = new ArrayList<>();
        for (StreamDefinition streamDefinition : siddhiManager.getStreamDefinitions()) {
            if (suitableToList(streamDefinition.getStreamId())) {
                StratioStreamingMessage message = new StratioStreamingMessage();
                for (Attribute attribute : streamDefinition.getAttributeList()) {
                    message.addColumn(new ColumnNameTypeValue(attribute.getName(), this.getStreamingType(attribute
                            .getType()), null));
                }
                StreamStatusDTO streamStatus = streamStatusDao.get(streamDefinition.getStreamId());

                if (streamStatus != null) {
                    Map<String, QueryDTO> attachedQueries = streamStatus.getAddedQueries();

                    for (Entry<String, QueryDTO> entry : attachedQueries.entrySet()) {
                        message.addQuery(new StreamQuery(entry.getKey(), entry.getValue().getQueryRaw()));
                    }
                    message.setUserDefined(streamStatus.getUserDefined());
                    message.setActiveActions(streamStatusDao.getEnabledActions(streamDefinition.getStreamId()));
                }

                message.setStreamName(streamDefinition.getStreamId());

                result.add(message);
            }
        }

        return result;
    }

    private boolean suitableToList(String streamName) {
        boolean startWithSinkPrefix = streamName.startsWith(STREAMING.STATS_NAMES.SINK_STREAM_PREFIX);
        boolean isAStatStream = Arrays.asList(STREAMING.STATS_NAMES.STATS_STREAMS).contains(streamName);

        return !startWithSinkPrefix && !isAStatStream;
    }

    @Timed(absolute = true, name = "streams.send.time")
    public void send(String streamName, List<ColumnNameTypeValue> columns) throws ServiceException {
        try {
            siddhiManager.getInputHandler(streamName).send(
                    SiddhiUtils.getOrderedValues(siddhiManager.getStreamDefinition(streamName), columns));
        } catch (InterruptedException e) {
            throw new ServiceException(String.format("Error sending data to stream %s, column data: %s", streamName,
                    columns), e);
        }

    }

    private Type getSiddhiType(ColumnType originalType) {
        switch (originalType) {
        case STRING:
            return Attribute.Type.STRING;
        case BOOLEAN:
            return Attribute.Type.BOOL;
        case DOUBLE:
            return Attribute.Type.DOUBLE;
        case INTEGER:
            return Attribute.Type.INT;
        case LONG:
            return Attribute.Type.LONG;
        case FLOAT:
            return Attribute.Type.FLOAT;
        default:
            throw new RuntimeException("Unsupported Column type: " + originalType);
        }
    }

    private ColumnType getStreamingType(Type type) {
        switch (type) {
        case STRING:
            return ColumnType.STRING;
        case BOOL:
            return ColumnType.BOOLEAN;
        case DOUBLE:
            return ColumnType.DOUBLE;
        case INT:
            return ColumnType.INTEGER;
        case LONG:
            return ColumnType.LONG;
        case FLOAT:
            return ColumnType.FLOAT;
        default:
            throw new RuntimeException("Unsupported Column type: " + type);
        }
    }
}
