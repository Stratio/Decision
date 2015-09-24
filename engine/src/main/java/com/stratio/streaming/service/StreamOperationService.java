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

import com.codahale.metrics.annotation.Timed;
import com.ryantenney.metrics.annotation.Counted;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.dao.StreamStatusDao;
import com.stratio.streaming.exception.ServiceException;
import org.wso2.siddhi.core.SiddhiManager;

import java.util.List;

public class StreamOperationService extends StreamOperationServiceWithoutMetrics {

    public StreamOperationService(SiddhiManager siddhiManager, StreamStatusDao streamStatusDao, CallbackService callbackService) {
        super(siddhiManager, streamStatusDao, callbackService);
    }

    @Override
    @Counted(absolute = true, name = "streams.total.created", monotonic = true)
    public void createStream(String streamName, List<ColumnNameTypeValue> columns) {
        super.createStream(streamName, columns);
    }

    @Override
    public boolean streamExist(String streamName) {
        return super.streamExist(streamName);
    }

    @Override
    public boolean isUserDefined(String streamName) {
        return super.isUserDefined(streamName);
    }

    @Override
    @Counted(absolute = true, name = "streams.total.altered", monotonic = true)
    public int enlargeStream(String streamName, List<ColumnNameTypeValue> columns) throws ServiceException {
        return super.enlargeStream(streamName, columns);
    }

    @Override
    @Counted(absolute = true, name = "streams.total.deleted", monotonic = true)
    public void dropStream(String streamName) {
        super.dropStream(streamName);
    }

    @Override
    @Counted(absolute = true, name = "queries.total.added", monotonic = true)
    public String addQuery(String streamName, String queryString) {
        return super.addQuery(streamName, queryString);
    }

    @Override
    @Counted(absolute = true, name = "queries.total.removed", monotonic = true)
    public void removeQuery(String queryId, String streamName) {
        super.removeQuery(queryId, streamName);
    }

    @Override
    public boolean queryIdExists(String streamName, String queryId) {
        return super.queryIdExists(streamName, queryId);
    }

    @Override
    public boolean queryRawExists(String streamName, String queryRaw) {
        return super.queryRawExists(streamName, queryRaw);
    }

    @Override
    public void enableAction(String streamName, StreamAction action) {
        super.enableAction(streamName, action);
    }

    @Override
    public void disableAction(String streamName, StreamAction action) {
        super.disableAction(streamName, action);
    }

    @Override
    public boolean isActionEnabled(String streamName, StreamAction action) {
        return super.isActionEnabled(streamName, action);
    }

    @Override
    @Counted(absolute = true, name = "streams.total.listed", monotonic = true)
    public List<StratioStreamingMessage> list() {
        return super.list();
    }

    @Override
    @Timed(absolute = true, name = "streams.send.time")
    public void send(String streamName, List<ColumnNameTypeValue> columns) throws ServiceException {
        super.send(streamName, columns);
    }
}
