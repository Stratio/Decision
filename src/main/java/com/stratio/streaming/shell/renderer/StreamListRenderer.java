package com.stratio.streaming.shell.renderer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.shell.ShellException;
import org.springframework.shell.support.table.TableRenderer;
import org.springframework.shell.support.util.OsUtils;

import com.stratio.streaming.commons.messages.StreamQuery;
import com.stratio.streaming.commons.streams.StratioStream;

public class StreamListRenderer implements Renderer<List<StratioStream>> {

    private static final String STREAM_NAME = "Stream name";
    private static final String USER_DEFINED = "User defined";
    private static final String QUERIES = "Queries";
    private static final String ELEMENTS = "Elements";

    private static final String ID = "Id";
    private static final String QUERY_RAW = "Query raw";

    private final static List<String> columns = Arrays.asList(STREAM_NAME, USER_DEFINED, QUERIES, ELEMENTS);
    private final static List<String> queryColumns = Arrays.asList(ID, QUERY_RAW);

    @Override
    public String render(List<StratioStream> streams) throws ShellException {
        StringBuilder queryTables = new StringBuilder();
        List<Map<String, Object>> data = new ArrayList<>();
        for (StratioStream stratioStream : streams) {
            Map<String, Object> row = new HashMap<>();
            row.put(STREAM_NAME, stratioStream.getStreamName());
            row.put(USER_DEFINED, stratioStream.getUserDefined());
            row.put(QUERIES, stratioStream.getQueries().size());
            row.put(ELEMENTS, stratioStream.getColumns().size());
            data.add(row);

            queryTables.append(renderQueriesTable(stratioStream.getQueries(), stratioStream.getStreamName()));
        }

        StringBuilder result = new StringBuilder();
        result.append(TableRenderer.renderMapDataAsTable(data, columns));
        result.append(OsUtils.LINE_SEPARATOR);
        result.append(queryTables);

        return result.toString();
    }

    private String renderQueriesTable(List<StreamQuery> queries, String streamName) {
        StringBuilder result = new StringBuilder();
        if (queries != null && queries.size() != 0) {
            List<Map<String, Object>> data = new ArrayList<>();
            for (StreamQuery streamQuery : queries) {
                Map<String, Object> row = new HashMap<>();
                row.put(ID, streamQuery.getQueryId());
                row.put(QUERY_RAW, streamQuery.getQuery());
                data.add(row);
            }
            result.append("Queries from stream ".concat(streamName).concat(":"));
            result.append(OsUtils.LINE_SEPARATOR);
            result.append(OsUtils.LINE_SEPARATOR);
            result.append(TableRenderer.renderMapDataAsTable(data, queryColumns));
            result.append(OsUtils.LINE_SEPARATOR);
        }
        return result.toString();
    }
}
