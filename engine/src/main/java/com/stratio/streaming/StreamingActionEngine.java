package com.stratio.streaming;

import com.stratio.streaming.configuration.BaseConfiguration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class StreamingActionEngine {

    private static Logger log = LoggerFactory.getLogger(StreamingActionEngine.class);

    public static void main(String [] args) {
        try (AnnotationConfigApplicationContext annotationConfigApplicationContext = new AnnotationConfigApplicationContext(
                BaseConfiguration.class)) {

            annotationConfigApplicationContext.registerShutdownHook();

            JavaStreamingContext actionContext = annotationConfigApplicationContext.getBean("actionContext", JavaStreamingContext.class);

            actionContext.start();
            log.info("Started Stratio Streaming Action context.");

            actionContext.awaitTermination();
        } catch (Exception e) {
            log.error("Fatal error", e);
        }
    }

}
