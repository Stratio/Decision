package com.stratio.streaming.shell.converter;

import java.util.Arrays;
import java.util.List;

import org.springframework.shell.ShellException;
import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;
import org.springframework.stereotype.Component;

import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.messaging.ColumnNameType;
import com.stratio.streaming.shell.converter.wrapper.ColumnNameTypeList;

@Component
public class ColumnNameTypeConverter implements Converter<ColumnNameTypeList> {

    @Override
    public boolean supports(Class<?> type, String optionContext) {
        if (type.equals(ColumnNameTypeList.class)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public ColumnNameTypeList convertFromText(String value, Class<?> targetType, String optionContext) {

        String[] keyValueArray = value.split(",");
        ColumnNameTypeList result = new ColumnNameTypeList();
        for (String keyValueString : keyValueArray) {
            String[] keyTypeArray = keyValueString.split("\\.");
            if (keyTypeArray.length != 2) {
                throw new ShellException("Error processing (".concat(keyValueString).concat(")"));
            }

            String key = keyTypeArray[0].trim();
            String type = keyTypeArray[1].trim();
            try {
                result.add(new ColumnNameType(key, ColumnType.valueOf(type.toUpperCase())));
            } catch (IllegalArgumentException e) {
                StringBuilder sb = new StringBuilder();
                sb.append("Type not found");
                sb.append(" (".concat(type).concat(") "));
                sb.append(" Available types: ".concat(Arrays.asList(ColumnType.values()).toString()));
                throw new ShellException(sb.toString());
            }
        }
        return result;

    }

    @Override
    public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType, String existingData,
            String optionContext, MethodTarget target) {
        return false;
    }

}
