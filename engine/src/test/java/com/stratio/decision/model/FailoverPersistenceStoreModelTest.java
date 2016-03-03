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
package com.stratio.decision.model;

import com.stratio.decision.service.StreamsHelper;
import com.stratio.decision.streams.StreamStatusDTO;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by aitor on 9/28/15.
 */
public class FailoverPersistenceStoreModelTest {

    private FailoverPersistenceStoreModel persistenceModel;

    private Map<String, StreamStatusDTO> streamStatuses= new HashMap<>();


    @Before
    public void setUp() throws Exception {
        Map<String, StreamStatusDTO> streamStatuses= new HashMap<>();
        streamStatuses.put(StreamsHelper.STREAM_NAME,
                new StreamStatusDTO(StreamsHelper.STREAM_NAME, true, StreamsHelper.COLUMNS));
        streamStatuses.put(StreamsHelper.STREAM_NAME2,
                new StreamStatusDTO(StreamsHelper.STREAM_NAME, true, StreamsHelper.COLUMNS2));
        persistenceModel= new FailoverPersistenceStoreModel(streamStatuses, null);
    }

    @Test
    public void testGetStreamStatuses() throws Exception {
        assertEquals("Expected same size of Stream Statuses", 2, persistenceModel.getStreamStatuses().size());
    }

    @Test
    public void testGetSiddhiSnapshot() throws Exception {
        assertNull("Expected null byte array", persistenceModel.getSiddhiSnapshot());

    }

    @Test
    public void testConstructorBytesArray() throws Exception {
        byte[] bytes= persistenceModel.FailOverPersistenceModelToByte();
        FailoverPersistenceStoreModel model= new FailoverPersistenceStoreModel(bytes);
        assertEquals("Expected size is not fine", 2, model.getStreamStatuses().size());
    }

    @Test
    public void testFailOverPersistenceModelToByte() throws Exception {
        byte[] bytes= persistenceModel.FailOverPersistenceModelToByte();
        assertTrue("Expected bytes array not found after convert", bytes.length>0);
    }

    @Test
    public void testHashCode() throws Exception {
        assertTrue("Expected hashcode higher than zero", persistenceModel.hashCode()!=0);
    }

    @Test
    public void testEquals() throws Exception {

        FailoverPersistenceStoreModel persistenceModel2= new
                FailoverPersistenceStoreModel(persistenceModel.getStreamStatuses(), null);

        assertTrue("Given object is not equals", persistenceModel.equals(persistenceModel2));
    }

    @Test
    public void testToString() throws Exception {
        assertTrue("Expected string is empty", persistenceModel.toString().length()>0);
    }
}