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
            List<StratioStream> streams = cachedStreamsDAO.listUncachedStreams();
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
