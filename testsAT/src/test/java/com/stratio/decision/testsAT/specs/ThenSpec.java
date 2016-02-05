package com.stratio.streaming.specs;

import static net.sf.expectit.matcher.Matchers.regexp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.notNullValue;
import static com.stratio.tests.utils.matchers.PatternMatcher.pattern;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.sf.expectit.Result;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import com.stratio.cucumber.converter.ArrayListConverter;
import com.stratio.cucumber.converter.NullableStringConverter;
import com.stratio.decision.commons.constants.ColumnType;
import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.exceptions.StratioAPIGenericException;
import com.stratio.decision.commons.exceptions.StratioEngineOperationException;
import com.stratio.decision.commons.exceptions.StratioEngineStatusException;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.commons.streams.StratioStream;
import com.stratio.decision.api.dto.StratioQueryStream;
import com.stratio.decision.testsAT.specs.BaseSpec;
import com.stratio.decision.testsAT.specs.Common;

import cucumber.api.DataTable;
import cucumber.api.Transform;
import cucumber.api.java.en.Then;

public class ThenSpec extends BaseSpec {

    public ThenSpec(Common spec) {
        this.commonspec = spec;
    }

    @Then("^the count of created streams is '(.*?)'$")
    public void assertStreamCount(Integer expectedCount)
            throws StratioEngineStatusException, StratioAPIGenericException {
        commonspec.getLogger().info("Verifying stream count");
        assertThat("Bad stream count", commonspec.getStratioStreamingAPI()
                .listStreams().size(), equalTo(expectedCount));
    }

    @Then("^the stream '(.*?)' has this columns \\(with name and type\\):$")
    public void assertStreamColumns(String streamName, DataTable data)
            throws StratioEngineOperationException {
        commonspec.getLogger().info("Verifying stream columns");

        List<ColumnNameTypeValue> columns = commonspec.getStratioStreamingAPI()
                .columnsFromStream(streamName);

        List<ColumnNameTypeValue> expectedColumns = new ArrayList<ColumnNameTypeValue>();
        for (List<String> row : data.raw()) {
            ColumnNameTypeValue expectedCol = new ColumnNameTypeValue(
                    row.get(0), ColumnType.valueOf(row.get(1).toUpperCase()),
                    null);
            expectedColumns.add(expectedCol);
        }

        assertThat(
                "Unexpected column count at stream " + streamName,
                commonspec.getStratioStreamingAPI().columnsFromStream(
                        streamName), hasSize(data.raw().size()));

        assertThat("Unexpected columns at stream " + streamName, columns,
                equalTo(expectedColumns));
    }

    @Then("^the stream '(.*?)' has this content \\(with column name, type and value\\):$")
    public void assertStreamContent(String streamName, DataTable data)
            throws StratioEngineOperationException, NoSuchFieldException,
            SecurityException, IllegalArgumentException, IllegalAccessException {

        KafkaStream<String, StratioStreamingMessage> streams = commonspec
                .getStreamListener();

        assertThat("No listener attached to stream " + streamName, streams,
                is(notNullValue()));

        ConsumerIterator<String, StratioStreamingMessage> consumer = streams
                .iterator();

        Field timeout = ConsumerIterator.class
                .getDeclaredField("consumerTimeoutMs");
        timeout.setAccessible(true);
        timeout.set(consumer, 10000);

        if (data == null) {
            StratioStreamingMessage msg = null;

            commonspec.getLogger().warn("Verifying empty stream content");
            try {
                while (consumer.hasNext()) {
                    msg = (StratioStreamingMessage) consumer.next().message();
                    assertThat("Unexpected stream content at " + streamName,
                            msg, is(nullValue()));
                }
            } catch (kafka.consumer.ConsumerTimeoutException e) {
                commonspec
                        .getLogger()
                        .info("Got expected timeout whilst consuming a kafka topic: {}",
                                streamName);
            }

        } else {
            commonspec.getLogger().info("Verifying stream content");

            List<String> expectedData = new ArrayList<String>();
            for (List<String> row : data.raw()) {
                String expectedRow = "";
                for (String e : row) {
                    String[] elem = e.split(" ");
                    expectedRow += elem[0] + " : "
                            + ColumnType.valueOf(elem[1].toUpperCase()) + " : "
                            + elem[2] + " | ";
                }
                expectedRow = expectedRow
                        .substring(0, expectedRow.length() - 3);
                expectedData.add(expectedRow);
            }

            Integer eventNum = 0;
            try {
                while (consumer.hasNext()) {
                    StratioStreamingMessage msg = (StratioStreamingMessage) consumer
                            .next().message();

                    eventNum++;
                    String got = "";
                    for (ColumnNameTypeValue col : msg.getColumns()) {
                        got += col.getColumn() + " : " + col.getType() + " : "
                                + col.getValue() + " | ";
                    }
                    got = got.substring(0, got.length() - 3);
                    commonspec.getLogger()
                            .info("Got event from {}", streamName);

                    assertThat("Unexpected stream content", got,
                            isIn(expectedData));
                }
            } catch (kafka.consumer.ConsumerTimeoutException e) {
                commonspec
                        .getLogger()
                        .warn("Got timeout whilst consuming a kafka topic {}. {} events read",
                                streamName, eventNum);
            }
        }
    }

    @Then("^the number of kafka topics is(, at least,)? '(.*?)'$")
    public void assertKakfaTopicsCount(String least, Integer expectedCount)
            throws IOException {
        commonspec.getLogger().info("Verifying topic count ");

        if (least == null) {
            assertThat("Bad topic count",
                    commonspec.pollZKForTopics("=", expectedCount),
                    equalTo(expectedCount));
        } else {
            assertThat("Bad topic count",
                    commonspec.pollZKForTopics(">=", expectedCount),
                    greaterThanOrEqualTo(expectedCount));
        }
    }

    @Then("^the stream '(.*?)' has '(.*?)' quer(?:y|ies)$")
    public void assertStreamQueriesCount(String streamName,
                                         Integer expectedCount) throws StratioEngineOperationException {
        commonspec.getLogger().info("Verifying stream's queries count");
        List<StratioQueryStream> queries = new ArrayList<StratioQueryStream>();
        queries = commonspec.getStratioStreamingAPI().queriesFromStream(
                streamName);
        assertThat("Unexpected queries found for stream " + streamName,
                queries.size(), equalTo(expectedCount));
    }

    @Then("^the stream '(.*?)' has this query: '(.*?)'$")
    public void assertStreamQueriesExistance(String streamName,
                                             String expectedQuery) throws StratioEngineOperationException {
        commonspec.getLogger().info("Verifying stream's queries count");
        List<StratioQueryStream> queries = new ArrayList<StratioQueryStream>();
        queries = commonspec.getStratioStreamingAPI().queriesFromStream(
                streamName);
        List<String> rawqueries = new ArrayList<String>();
        for (StratioQueryStream query : queries) {
            rawqueries.add(query.query());
        }

        assertThat("Unexpected queries found for stream " + streamName,
                rawqueries, hasItem(expectedQuery));
    }

    @Then("^the stream '(.*?)' exists$")
    public void assertStreamExist(String stream)
            throws StratioEngineStatusException, StratioAPIGenericException {
        commonspec.getLogger().info("Verifying stream existance ");

        List<String> existingStreams = new ArrayList<String>();
        List<StratioStream> listed = commonspec.getStratioStreamingAPI()
                .listStreams();

        for (StratioStream s : listed) {
            existingStreams.add(s.getStreamName());
        }
        assertThat("Unexistant stream", existingStreams, hasItem(stream));
    }

    @Then("^the stream '(.*?)' has '(.*?)' as active actions$")
    public void assertStreamActions(
            @Transform(NullableStringConverter.class) String stream,
            @Transform(ArrayListConverter.class) ArrayList<String> expectedActions)
            throws StratioEngineStatusException, StratioAPIGenericException {
        commonspec.getLogger().info("Verifying stream existance ");

        List<StratioStream> listedStreams = commonspec.getStratioStreamingAPI()
                .listStreams();

        Set<StreamAction> actions = null;

        for (StratioStream s : listedStreams) {
            if (s.getStreamName().equals(stream)) {
                actions = s.getActiveActions();
            }
        }

        assertThat("No action gotten, maybe there's no stream?", actions,
                is(notNullValue()));

        Iterator<StreamAction> sAactions = actions.iterator();
        String[] sActions = new String[actions.size()];
        int i = 0;
        while (sAactions.hasNext()) {
            sActions[i] = sAactions.next().toString();
        }
        assertThat("Unexistant action", expectedActions, hasItems(sActions));
    }

    @Then("^the shell must output the string '(.*?)'$")
    public void assertShellOutput(String expectedOutput) throws IOException {
        commonspec.getLogger().info("Verifying shell output");

        expectedOutput = expectedOutput.replaceAll("\\\\'", "'");

        String re = expectedOutput.replaceAll("[^\\.]\\*|^\\*", "\\\\*");
        re = re.replaceAll("\\(", "\\\\(");
        String escapedre = re.replaceAll("\\)", "\\\\)");
        re = "(" + escapedre + ")\n";

        Result result = commonspec.getShellIface().expect(regexp(re));
        String o = "";
        if (result.groupCount() > 0) {
            o = result.group(1);
        } else {
            // last line in case non-existing match
            o = commonspec.getShellIface().expect(regexp("$")).getBefore();
        }

        assertThat("Shell output not found. Last output was", o,
                pattern(escapedre));
    }

    @Then("^the shell output matches a regexp like '(.*?)'$")
    public void assertShellregexp(String re) throws IOException {
        commonspec.getLogger().info("Getting a matched element ");
        // String version =
        // commonspec.getShellIface().expect(regexp("(.*?)\n")).group(1);
        String elem = commonspec.getShellIface().expect(regexp(re)).group(1);
        commonspec.getLogger().info("captured element: {}", elem);
        // commonspec.setShellValue(val);
    }
}