package com.stratio.streaming;

import com.stratio.streaming.configuration.BaseConfiguration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Map;

public class StreamingDataEngine {

    private static Logger log = LoggerFactory.getLogger(StreamingDataEngine.class);

    public static void main(String [] args) {
        try (AnnotationConfigApplicationContext annotationConfigApplicationContext = new AnnotationConfigApplicationContext(
                BaseConfiguration.class)) {

            annotationConfigApplicationContext.registerShutdownHook();

            JavaStreamingContext dataContext = annotationConfigApplicationContext.getBean("dataContext", JavaStreamingContext.class);

            dataContext.start();
            log.info("Started Stratio Streaming Data context.");

            dataContext.awaitTermination();
        } catch (Exception e) {
            log.error("Fatal error", e);
        }
    }

}
