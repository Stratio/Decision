package com.stratio.decision.functions;

import org.apache.spark.api.java.JavaRDD;

import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.service.SaveToCassandraOperationsService;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by josepablofernandez on 8/06/16.
 */
public class CheckCassandraStructureFunction implements
        Function<JavaRDD<StratioStreamingMessage>, Void> {

    private static Logger log = LoggerFactory.getLogger(CheckCassandraStructureFunction.class);

    private  SaveToCassandraOperationsService cassandraTableOperationsService;

    @Override public Void call(JavaRDD<StratioStreamingMessage> v1) throws Exception {

        if (!v1.isEmpty()) {
            StratioStreamingMessage message = v1.first();
            getCassandraTableOperationsService().checkStructure(message);
        }

        return null;
    }

    private SaveToCassandraOperationsService getCassandraTableOperationsService() {
        if (cassandraTableOperationsService == null) {
            cassandraTableOperationsService = (SaveToCassandraOperationsService) ActionBaseContext.getInstance()
                    .getContext().getBean
                            ("saveToCassandraOperationsService");

        }

        return cassandraTableOperationsService;
    }
}
