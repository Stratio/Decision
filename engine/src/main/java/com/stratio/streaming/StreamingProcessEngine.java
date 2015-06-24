package com.stratio.streaming;

import com.stratio.streaming.configuration.BaseConfiguration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class StreamingProcessEngine {

    private static Logger log = LoggerFactory.getLogger(StreamingProcessEngine.class);

    public static void main(String [] args) {
        try (AnnotationConfigApplicationContext annotationConfigApplicationContext = new AnnotationConfigApplicationContext(
                BaseConfiguration.class)) {

            annotationConfigApplicationContext.registerShutdownHook();

            JavaStreamingContext processContext = annotationConfigApplicationContext.getBean("processContext", JavaStreamingContext.class);

            processContext.start();
            log.info("Started Stratio Streaming Action context.");

            processContext.awaitTermination();
        } catch (Exception e) {
            log.error("Fatal error", e);
        }
    }

}
