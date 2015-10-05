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
package com.stratio.decision.shell.renderer;

import com.stratio.decision.commons.messages.StreamQuery;
import com.stratio.decision.commons.streams.StratioStream;
import org.springframework.shell.ShellException;
import org.springframework.shell.support.table.TableRenderer;
import org.springframework.shell.support.util.OsUtils;

import java.util.*;

public class StreamListRenderer implements Renderer<List<StratioStream>> {

    private static final String STREAM_NAME = "Stream name";
    private static final String USER_DEFINED = "User defined";
    private static final String QUERIES = "Queries";
    private static final String ELEMENTS = "Elements";
    private static final String ACTIVE_ACTIONS = "Active actions";

    private static final String ID = "Id";
    private static final String QUERY_RAW = "Query raw";

    private final static List<String> columns = Arrays.asList(STREAM_NAME, USER_DEFINED, QUERIES, ELEMENTS,
            ACTIVE_ACTIONS);
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
            row.put(ACTIVE_ACTIONS, stratioStream.getActiveActions().toString());

            queryTables.append(renderQueriesTable(stratioStream.getQueries(), stratioStream.getStreamName()));

            data.add(row);
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
