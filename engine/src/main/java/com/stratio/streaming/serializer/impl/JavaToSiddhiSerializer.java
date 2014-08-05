package com.stratio.streaming.serializer.impl;

import java.util.ArrayList;
import java.util.List;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;

import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.serializer.Serializer;
import com.stratio.streaming.service.StreamMetadataService;

public class JavaToSiddhiSerializer implements Serializer<StratioStreamingMessage, Event> {

    private static final long serialVersionUID = 1694881934063941893L;

    private final StreamMetadataService streamMetadataService;

    public JavaToSiddhiSerializer(StreamMetadataService streamMetadataService) {
        this.streamMetadataService = streamMetadataService;
    }

    @Override
    public Event serialize(StratioStreamingMessage object) {
        Object[] values = new Object[object.getColumns().size()];
        for (ColumnNameTypeValue column : object.getColumns()) {
            values[streamMetadataService.getAttributePosition(object.getStreamName(), column.getColumn())] = column
                    .getValue();
        }
        return new InEvent(object.getStreamName(), System.currentTimeMillis(), values);
    }

    @Override
    public StratioStreamingMessage deserialize(Event object) {
        List<ColumnNameTypeValue> columns = new ArrayList<>();
        for (int i = 0; i < object.getData().length; i++) {
            Object data = object.getData()[i];
            Attribute attribute = streamMetadataService.getAttribute(object.getStreamId(), i);
            columns.add(new ColumnNameTypeValue(attribute.getName(), encodeSiddhiType(attribute.getType()), data));
        }
        return new StratioStreamingMessage(object.getStreamId(), object.getTimeStamp(), columns);
    }

    @Override
    public List<Event> serialize(List<StratioStreamingMessage> object) {
        List<Event> result = new ArrayList<>();
        if (object != null) {
            for (StratioStreamingMessage message : object) {
                result.add(serialize(message));
            }
        }
        return result;
    }

    @Override
    public List<StratioStreamingMessage> deserialize(List<Event> object) {
        List<StratioStreamingMessage> result = new ArrayList<>();
        if (object != null) {
            for (Event event : object) {
                result.add(deserialize(event));
            }
        }
        return result;
    }

    private ColumnType encodeSiddhiType(Type type) {
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
