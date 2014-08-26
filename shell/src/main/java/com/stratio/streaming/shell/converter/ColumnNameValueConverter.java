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

import java.util.List;

import org.springframework.shell.ShellException;
import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;
import org.springframework.stereotype.Component;

import com.stratio.streaming.api.messaging.ColumnNameValue;
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
