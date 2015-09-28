package com.stratio.streaming.model;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.stratio.streaming.service.StreamsHelper;
import com.stratio.streaming.streams.StreamStatusDTO;

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