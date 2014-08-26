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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;
import org.springframework.stereotype.Component;

import com.stratio.streaming.commons.streams.StratioStream;
import com.stratio.streaming.shell.dao.CachedStreamsDAO;

@Component
public class StreamConverter implements Converter<String> {

    private static Logger log = LoggerFactory.getLogger(StreamConverter.class);

    @Autowired
    private CachedStreamsDAO cachedStreamsDAO;

    @Override
    public boolean supports(Class<?> type, String optionContext) {
        return String.class.isAssignableFrom(type) && (optionContext != null && optionContext.contains("stream"));
    }

    @Override
    public String convertFromText(String value, Class<?> targetType, String optionContext) {
        return value;
    }

    @Override
    public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType, String existingData,
            String optionContext, MethodTarget target) {
        log.info("Listing possible values. TargetType: {}, Existing data: {}, Option context: {}, Target {}",
                targetType, existingData, optionContext, target);
        boolean completedValues = false;
        try {
            List<StratioStream> streams = cachedStreamsDAO.listStreams();
            log.info("Listed {} streams", streams.size());
            for (int i = 0; i < streams.size(); i++) {
                if (existingData.equals("") || streams.get(i).getStreamName().startsWith(existingData)) {
                    log.info("Stream {} start with {}", streams.get(i).getStreamName(), existingData);
                    completions.add(new Completion(streams.get(i).getStreamName()));
                    if (existingData.equals(streams.get(i).getStreamName())) {
                        log.info("Stream match! {} ", streams.get(i).getStreamName());
                        break;
                    }
                }
            }
            return completedValues;
        } catch (Exception e) {
            log.error("Error reading streams", e);
            return false;
        }
    }
}
