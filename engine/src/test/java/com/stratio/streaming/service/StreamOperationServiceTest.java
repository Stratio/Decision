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
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.StreamEvent;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;

import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.dao.StreamStatusDao;
import com.stratio.streaming.exception.ServiceException;


public class StreamOperationServiceTest {

    private SiddhiManager sm;

    private StreamOperationService streamOperationService;

    private CallbackService callbackFactory;

    private StreamStatusDao streamStatusDao;

    private static final String STREAM_NAME_GOOD = "goodStreamName";

    private static final String INFERRED_STREAM_GOOD = "goodInferredStreamName";

    @Before
    public void setUp() {
        streamStatusDao = Mockito.mock(StreamStatusDao.class);
        callbackFactory = Mockito.mock(CallbackService.class);

        sm = new SiddhiManager();
        streamOperationService = new StreamOperationService(sm, streamStatusDao, callbackFactory);
    }

    @Test
    public void createStreamTest() {
        createBaseStream();

        Assert.assertEquals("Expected value not found", 1, sm.getStreamDefinitions().size());

        Assert.assertEquals("Expected value not found", 2,
                sm.getStreamDefinitions().get(0).getAttributeList().size());
    }

    @Test
    public void enlargeStreamTest() throws ServiceException {
        createBaseStream();

        Assert.assertEquals("Expected value not found", 1, sm.getStreamDefinitions().size());
        Assert.assertEquals("Expected value not found", 2,
                sm.getStreamDefinitions().get(0).getAttributeList().size());

        List<ColumnNameTypeValue> columns = new ArrayList<>();
        columns.add(new ColumnNameTypeValue("col3", ColumnType.INTEGER, null));

        streamOperationService.enlargeStream(STREAM_NAME_GOOD, columns);

        Assert.assertEquals("Expected value not found", 1, sm.getStreamDefinitions().size());
        Assert.assertEquals("Expected value not found", 3,
                sm.getStreamDefinitions().get(0).getAttributeList().size());
    }

    @Test
    public void addQueryTest() {
        createBaseStream();

        streamOperationService.addQuery(STREAM_NAME_GOOD, "from " + STREAM_NAME_GOOD + " select * insert into "
                + INFERRED_STREAM_GOOD + " for current-events");

        Mockito.verify(streamStatusDao, VerificationModeFactory.times(2)).createInferredStream(Mockito.anyString(), Mockito.anyList());

        Assert.assertEquals("Expected value not found", 2, sm.getStreamDefinitions().size());

    }

    @Test
    public void addCallbackFunctionTest() throws ServiceException {

        StreamAction streamAction = StreamAction.INDEXED;

        ArgumentCaptor<String> actionQueryIdArgumentCaptor = ArgumentCaptor.forClass(String.class);

        QueryCallback callback = Mockito.mock(QueryCallback.class);

        Mockito.when(callbackFactory.add(Mockito.anyString(), (Set<StreamAction>) Mockito.anyObject())).thenReturn(
                callback);

        Mockito.doNothing().when(streamStatusDao).enableAction(Mockito.eq(STREAM_NAME_GOOD), Mockito.eq(streamAction));

        Mockito.doNothing().when(streamStatusDao)
                .setActionQuery(Mockito.eq(STREAM_NAME_GOOD), actionQueryIdArgumentCaptor.capture());

        createBaseStream();

        streamOperationService.enableAction(STREAM_NAME_GOOD, streamAction);

        List<ColumnNameTypeValue> columns = new ArrayList<>();
        columns.add(new ColumnNameTypeValue("col1", ColumnType.INTEGER, 34));

        streamOperationService.send(STREAM_NAME_GOOD, columns);

        Mockito.when(streamStatusDao.getActionQuery(STREAM_NAME_GOOD)).thenReturn(
                actionQueryIdArgumentCaptor.getValue());

        streamOperationService.disableAction(STREAM_NAME_GOOD, streamAction);

        streamOperationService.send(STREAM_NAME_GOOD, columns);

        Mockito.verify(callback, Mockito.times(1)).receiveStreamEvent(Mockito.anyLong(), (StreamEvent) Mockito.any(),
                (StreamEvent) Mockito.any());
    }

    private void createBaseStream() {
        List<ColumnNameTypeValue> columns = new ArrayList<>();
        columns.add(new ColumnNameTypeValue("col1", ColumnType.INTEGER, 1));
        columns.add(new ColumnNameTypeValue("col2", ColumnType.STRING, "test string"));

        streamOperationService.createStream(STREAM_NAME_GOOD, columns);
    }
}
