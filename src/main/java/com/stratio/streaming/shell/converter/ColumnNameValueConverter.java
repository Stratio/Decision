package com.stratio.streaming.shell.converter;

import java.util.List;

import org.springframework.shell.ShellException;
import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;
import org.springframework.stereotype.Component;

import com.stratio.streaming.messaging.ColumnNameValue;
import com.stratio.streaming.shell.converter.wrapper.ColumnNameValueList;

@Component
public class ColumnNameValueConverter implements Converter<ColumnNameValueList> {

    @Override
    public boolean supports(Class<?> type, String optionContext) {
        if (type.equals(ColumnNameValueList.class)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public ColumnNameValueList convertFromText(String shellValue, Class<?> targetType, String optionContext) {
        String[] keyValueArray = shellValue.split(",");
        ColumnNameValueList result = new ColumnNameValueList();
        for (String keyValueString : keyValueArray) {
            String[] nameValueArray = keyValueString.split("\\.");
            if (nameValueArray.length != 2) {
                throw new ShellException("Error processing (".concat(keyValueString).concat(")"));
            }

            String name = nameValueArray[0].trim();
            String value = nameValueArray[1].trim();
            result.add(new ColumnNameValue(name, value));
        }
        return result;

    }

    @Override
    public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType, String existingData,
            String optionContext, MethodTarget target) {
        return false;
    }

}
