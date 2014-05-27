package com.stratio.streaming.shell.renderer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.shell.ShellException;
import org.springframework.shell.support.table.TableRenderer;

import com.stratio.streaming.commons.messages.StreamQuery;
import com.stratio.streaming.commons.streams.StratioStream;

public class StreamListRenderer implements Renderer<List<StratioStream>> {

    private static final String NAME = "Name";
    private static final String USER_DEFINED = "User defined";
    private static final String QUERIES = "Queries";
    private static final String ELEMENTS = "Elements";

    private static final String ID = "Id";
    private static final String QUERY_RAW = "Query raw";

    private final static List<String> columns = Arrays.asList(NAME, USER_DEFINED, QUERIES, ELEMENTS);
    private final static List<String> queryColumns = Arrays.asList(ID, QUERY_RAW);

    @Override
    public String render(List<StratioStream> streams) throws ShellException {
        List<Map<String, Object>> data = new ArrayList<>();

        for (StratioStream stratioStream : streams) {
            Map<String, Object> row = new HashMap<>();
            row.put(NAME, stratioStream.getStreamName());
            row.put(USER_DEFINED, stratioStream.getUserDefined());
            row.put(QUERIES, renderQueriesTable(stratioStream.getQueries()));
            row.put(ELEMENTS, stratioStream.getColumns().size());

            data.add(row);
        }
        return TableRenderer.renderMapDataAsTable(data, columns);
    }

    private String renderQueriesTable(List<StreamQuery> queries) {
        String result;
        if (queries != null && queries.size() != 0) {
            List<Map<String, Object>> data = new ArrayList<>();
            for (StreamQuery streamQuery : queries) {
                Map<String, Object> row = new HashMap<>();
                row.put(ID, streamQuery.getQueryId());
                row.put(QUERY_RAW, streamQuery.getQuery());
                data.add(row);
            }
            result = TableRenderer.renderMapDataAsTable(data, queryColumns);
        } else {
            result = "";
        }
        return result;
    }
}
