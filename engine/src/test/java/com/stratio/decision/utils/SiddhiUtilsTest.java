package com.stratio.decision.utils;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import com.stratio.decision.commons.constants.ColumnType;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;

/**
 * Created by aitor on 10/7/15.
 */
public class SiddhiUtilsTest {

    private List<Attribute> listAttributes;

    @Before
    public void setUp() throws Exception {
        listAttributes= new ArrayList<Attribute>();
        listAttributes.add(new Attribute("myAttr", Attribute.Type.STRING));
        listAttributes.add(new Attribute("myAttr2", Attribute.Type.BOOL));
        listAttributes.add(new Attribute("myAttr3", Attribute.Type.DOUBLE));
        listAttributes.add(new Attribute("myAttr4", Attribute.Type.INT));
        listAttributes.add(new Attribute("myAttr5", Attribute.Type.LONG));
        listAttributes.add(new Attribute("myAttr6", Attribute.Type.FLOAT));
    }

    @Test
    public void testColumnAlreadyExistsInStream() throws Exception {
        String columnName= "myAttr";

        StreamDefinition definition= Mockito.mock(StreamDefinition.class);
        when(definition.getAttributeList()).thenReturn(listAttributes);

        assertTrue("Expected attribute not found", SiddhiUtils.columnAlreadyExistsInStream(columnName, definition));
        assertFalse("Unexpected attribute found", SiddhiUtils.columnAlreadyExistsInStream("NotExists", definition));
    }

    @Test
    public void testGetOrderedValues() throws Exception {
        List<ColumnNameTypeValue> columns = new ArrayList<>();
        columns.add(new ColumnNameTypeValue("myAttr", ColumnType.STRING, "test"));
        columns.add(new ColumnNameTypeValue("myAttr2", ColumnType.BOOLEAN, true));
        columns.add(new ColumnNameTypeValue("myAttr3", ColumnType.DOUBLE, 3.1));

        StreamDefinition definition= Mockito.mock(StreamDefinition.class);
        when(definition.getAttributeList()).thenReturn(listAttributes);
        when(definition.getAttributePosition("myAttr")).thenReturn(0);
        when(definition.getAttributePosition("myAttr2")).thenReturn(1);
        when(definition.getAttributePosition("myAttr3")).thenReturn(2);

        doReturn(Attribute.Type.STRING).when(definition).getAttributeType("myAttr");
        doReturn(Attribute.Type.BOOL).when(definition).getAttributeType("myAttr2");
        doReturn(Attribute.Type.DOUBLE).when(definition).getAttributeType("myAttr3");

        Object[] result= SiddhiUtils.getOrderedValues(definition, columns);
        assertEquals("Unexpected columns size", 6, result.length);
    }

    @Test
    public void testDecode() throws Exception {
        assertEquals("Unexpected result", "text", SiddhiUtils.decodeSiddhiValue("text", Attribute.Type.STRING));
        assertEquals("Unexpected result", true, SiddhiUtils.decodeSiddhiValue("true", Attribute.Type.BOOL));
        assertEquals("Unexpected result", 12.1, SiddhiUtils.decodeSiddhiValue("12.1", Attribute.Type.DOUBLE));
        assertEquals("Unexpected result", 12, SiddhiUtils.decodeSiddhiValue("12", Attribute.Type.INT));
        assertEquals("Unexpected result", 7L, SiddhiUtils.decodeSiddhiValue("7", Attribute.Type.LONG));
        assertEquals("Unexpected result", 22f, SiddhiUtils.decodeSiddhiValue("22", Attribute.Type.FLOAT));

        Double d= 22d;
        assertEquals("Unexpected result", "22.0", SiddhiUtils.decodeSiddhiValue(d, Attribute.Type.STRING));
        assertEquals("Unexpected result", d, SiddhiUtils.decodeSiddhiValue(d, Attribute.Type.DOUBLE));
        assertEquals("Unexpected result", 22, SiddhiUtils.decodeSiddhiValue(d, Attribute.Type.INT));
        assertEquals("Unexpected result", 22L, SiddhiUtils.decodeSiddhiValue(d, Attribute.Type.LONG));
        assertEquals("Unexpected result", 22f, SiddhiUtils.decodeSiddhiValue(d, Attribute.Type.FLOAT));
    }
}