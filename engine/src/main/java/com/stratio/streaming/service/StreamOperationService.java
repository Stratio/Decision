package com.stratio.streaming.service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.QueryFactory;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.AttributeAlreadyExistException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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

    private final static String VOID_STREAM_PREFIX = "VOID_";

    public StreamOperationService(SiddhiManager siddhiManager, StreamStatusDao streamStatusDao,
            CallbackService callbackService) {
        this.siddhiManager = siddhiManager;
        this.streamStatusDao = streamStatusDao;
        this.callbackService = callbackService;
    }

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

    public int enlargeStream(String streamName, List<ColumnNameTypeValue> columns) {
        int addedColumns = 0;
        StreamDefinition streamMetaData = siddhiManager.getStreamDefinition(streamName);

        for (ColumnNameTypeValue columnNameTypeValue : columns) {
            // Siddhi will throw an exception if you try to add a column that
            // already exists,
            // so we first try to find it in the stream
            if (!SiddhiUtils.columnAlreadyExistsInStream(columnNameTypeValue.getColumn(), streamMetaData)) {

                addedColumns++;
                streamMetaData.attribute(columnNameTypeValue.getColumn(), getSiddhiType(columnNameTypeValue.getType()));

            } else {
                throw new AttributeAlreadyExistException(columnNameTypeValue.getColumn());
            }
        }

        return addedColumns;
    }

    public void dropStream(String streamName) {

        Map<String, QueryDTO> attachedQueries = streamStatusDao.get(streamName).getAddedQueries();
        for (String queryId : attachedQueries.keySet()) {
            siddhiManager.removeQuery(queryId);
        }
        siddhiManager.removeStream(streamName);
        streamStatusDao.remove(streamName);
    }

    public void addQuery(String streamName, String queryString) {
        String queryId = siddhiManager.addQuery(queryString);
        streamStatusDao.addQuery(streamName, queryId, queryString);
        for (StreamDefinition streamDefinition : siddhiManager.getStreamDefinitions()) {
            // TODO refactor to obtain exactly siddhi inferred streams.
            streamStatusDao.createInferredStream(streamDefinition.getStreamId());
        }
    }

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

    public boolean queryExists(String streamName, String queryRaw) {
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
                    .from(QueryFactory.inputStream(streamName)).insertInto(VOID_STREAM_PREFIX.concat(streamName)));

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

    // TODO refactor
    public List<StratioStreamingMessage> listStreams() {
        List<StratioStreamingMessage> streams = Lists.newArrayList();
        for (StreamDefinition streamMetaData : siddhiManager.getStreamDefinitions()) {
            if (!Arrays.asList(STREAMING.STATS_NAMES.STATS_STREAMS).contains(streamMetaData.getStreamId())) {
                List<ColumnNameTypeValue> columns = Lists.newArrayList();
                List<StreamQuery> queries = Lists.newArrayList();
                Set<StreamAction> actions = Sets.newHashSet();
                boolean isUserDefined = false;
                for (Attribute column : streamMetaData.getAttributeList()) {
                    columns.add(new ColumnNameTypeValue(column.getName(),
                            SiddhiUtils.encodeSiddhiType(column.getType()), null));
                }
                StreamStatusDTO streamStatus = streamStatusDao.get(streamMetaData.getStreamId());

                if (streamStatus != null) {
                    Map<String, QueryDTO> attachedQueries = streamStatus.getAddedQueries();

                    for (Entry<String, QueryDTO> entry : attachedQueries.entrySet()) {
                        queries.add(new StreamQuery(entry.getKey(), entry.getValue().getQueryRaw()));
                    }
                    isUserDefined = streamStatus.getUserDefined();
                    actions = streamStatusDao.getEnabledActions(streamMetaData.getStreamId());
                }
                StratioStreamingMessage streamMessage = new StratioStreamingMessage(streamMetaData.getId(), columns,
                        queries);
                streamMessage.setUserDefined(isUserDefined);
                streamMessage.setActiveActions(actions);

                streams.add(streamMessage);
            }
        }
        return streams;
    }

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
}
