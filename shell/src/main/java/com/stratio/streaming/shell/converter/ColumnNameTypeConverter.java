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
