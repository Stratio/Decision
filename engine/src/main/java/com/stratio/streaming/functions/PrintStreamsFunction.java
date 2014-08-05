package com.stratio.streaming.functions;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.service.StreamOperationService;

public class PrintStreamsFunction implements Function<JavaRDD<Long>, Void> {

    private static final long serialVersionUID = 4173968709990244021L;

    private static Logger log = LoggerFactory.getLogger(PrintStreamsFunction.class);

    private final StreamOperationService streamOperationService;

    private static final String newLine = System.getProperty("line.separator");

    private static final String tab = "    ";

    public PrintStreamsFunction(StreamOperationService streamOperationService) {
        this.streamOperationService = streamOperationService;
    }

    @Override
    public Void call(JavaRDD<Long> arg0) throws Exception {
        StringBuilder sb = new StringBuilder();

        List<StratioStreamingMessage> streams = streamOperationService.list();

        sb.append(newLine).append("Shiddi Streams:").append(newLine);
        sb.append(tab).append("Total: ").append(streams.size()).append(newLine);
        sb.append(tab).append("Data:").append(newLine);

        for (StratioStreamingMessage stream : streams) {
            sb.append(tab).append(tab).append(stream.getStreamName()).append(" ").append(stream.getActiveActions())
                    .append(" ").append(stream.isUserDefined()).append(newLine);

            sb.append(tab).append(tab).append(tab).append("Columns: ").append("|");
            for (ColumnNameTypeValue column : stream.getColumns()) {
                sb.append(column.getColumn()).append("|");
            }
            sb.append(newLine);

        }

        log.info(sb.toString());
        return null;
    }
}
