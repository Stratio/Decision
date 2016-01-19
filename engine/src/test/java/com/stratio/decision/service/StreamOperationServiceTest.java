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
package com.stratio.decision.service;

import com.stratio.decision.commons.constants.ColumnType;
import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.configuration.ConfigurationContext;
import com.stratio.decision.dao.StreamStatusDao;
import com.stratio.decision.exception.ServiceException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.StreamEvent;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class StreamOperationServiceTest {

    private SiddhiManager sm;

    private StreamOperationService streamOperationService;

    private CallbackService callbackFactory;

    private StreamStatusDao streamStatusDao;

    private ConfigurationContext configurationContext;

    private static final String STREAM_NAME_GOOD = "goodStreamName";

    private static final String INFERRED_STREAM_GOOD = "goodInferredStreamName";

    private static final String CLUSTER_ID = "default";

    @Before
    public void setUp() {
        streamStatusDao = Mockito.mock(StreamStatusDao.class);
        callbackFactory = Mockito.mock(CallbackService.class);
        configurationContext = Mockito.mock(ConfigurationContext.class);

        sm = new SiddhiManager();
        streamOperationService = new StreamOperationService(sm, streamStatusDao, callbackFactory, configurationContext);
    }

    @Test
    public void createStreamTest() {
        createBaseStream();

        Assert.assertEquals("Expected value not found", 1, sm.getStreamDefinitions().size());

        Assert.assertEquals("Expected value not found", 6,
                sm.getStreamDefinitions().get(0).getAttributeList().size());
    }

    @Test
    public void enlargeStreamTest() throws ServiceException {
        createBaseStream();

        Assert.assertEquals("Expected value not found", 1, sm.getStreamDefinitions().size());
        Assert.assertEquals("Expected value not found", 6,
                sm.getStreamDefinitions().get(0).getAttributeList().size());

        List<ColumnNameTypeValue> columns = new ArrayList<>();
        columns.add(new ColumnNameTypeValue("col7", ColumnType.INTEGER, null));

        streamOperationService.enlargeStream(STREAM_NAME_GOOD, columns);

        Assert.assertEquals("Expected value not found", 1, sm.getStreamDefinitions().size());
        Assert.assertEquals("Expected value not found", 7,
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

        Mockito.when(callbackFactory.add(Mockito.anyString(), (Set<StreamAction>) Mockito.anyObject(), Mockito
                .anyString() )).thenReturn(
                callback);

        Mockito.doNothing().when(streamStatusDao).enableAction(Mockito.eq(STREAM_NAME_GOOD), Mockito.eq(streamAction));

        Mockito.doNothing().when(streamStatusDao)
                .setActionQuery(Mockito.eq(STREAM_NAME_GOOD), actionQueryIdArgumentCaptor.capture());


        Mockito.when(configurationContext.getClusterId()).thenReturn(
                CLUSTER_ID);

        createBaseStream();

        streamOperationService.enableAction(STREAM_NAME_GOOD, streamAction);

        List<ColumnNameTypeValue> columns = new ArrayList<>();
        columns.add(new ColumnNameTypeValue("col1", ColumnType.INTEGER, 34));
        columns.add(new ColumnNameTypeValue("col2", ColumnType.STRING, "text value"));
        columns.add(new ColumnNameTypeValue("col3", ColumnType.BOOLEAN, true));
        columns.add(new ColumnNameTypeValue("col4", ColumnType.DOUBLE, 1.2));
        columns.add(new ColumnNameTypeValue("col5", ColumnType.FLOAT, 10f));
        columns.add(new ColumnNameTypeValue("col6", ColumnType.LONG, 2L));

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
        columns.add(new ColumnNameTypeValue("col3", ColumnType.BOOLEAN, true));
        columns.add(new ColumnNameTypeValue("col4", ColumnType.DOUBLE, 1.3));
        columns.add(new ColumnNameTypeValue("col5", ColumnType.FLOAT, 20f));
        columns.add(new ColumnNameTypeValue("col6", ColumnType.LONG, 1L));

        streamOperationService.createStream(STREAM_NAME_GOOD, columns);
    }
}
