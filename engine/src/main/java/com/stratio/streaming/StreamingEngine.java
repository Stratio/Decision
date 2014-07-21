package com.stratio.streaming;

import java.util.Map;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.stratio.streaming.configuration.BaseConfiguration;

public class StreamingEngine {

    private static Logger log = LoggerFactory.getLogger(StreamingEngine.class);

    public static void main(String[] args) {
        try (AnnotationConfigApplicationContext annotationConfigApplicationContext = new AnnotationConfigApplicationContext(
                BaseConfiguration.class)) {
            annotationConfigApplicationContext.registerShutdownHook();

            Map<String, JavaStreamingContext> contexts = annotationConfigApplicationContext
                    .getBeansOfType(JavaStreamingContext.class);

            for (JavaStreamingContext context : contexts.values()) {
                context.start();
                log.info("Started context {}", context.sparkContext().appName());
            }
            contexts.get("actionContext").awaitTermination();
        }
    }
}
