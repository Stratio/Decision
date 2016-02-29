package com.stratio.decision.testsAT.specs;

import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import kafka.consumer.KafkaStream;

import com.stratio.cucumber.converter.NullableStringConverter;
import com.stratio.cucumber.converter.NullableIntegerConverter;
import com.stratio.decision.api.IStratioStreamingAPI;
import com.stratio.decision.api.StratioStreamingAPIFactory;
import com.stratio.decision.api.messaging.ColumnNameType;
import com.stratio.decision.api.messaging.ColumnNameValue;
import com.stratio.decision.commons.constants.ColumnType;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.commons.messages.StreamQuery;
import com.stratio.decision.commons.streams.StratioStream;

import cucumber.api.DataTable;
import cucumber.api.Transform;
import cucumber.api.java.en.When;

public class WhenSpec extends BaseSpec {

    public WhenSpec(Common spec) {
        this.commonspec = spec;
    }

    @When("^I create a stream with name '(.*?)' and columns \\(with type\\):$")
    public void streamCreate(
            @Transform(NullableStringConverter.class) String streamName,
            DataTable table) {
        commonspec.getLogger().info("New stream {} creation request",
                streamName);

        ColumnNameType streamColumn;
        List<ColumnNameType> columnList = null;
        if (!("").equals(table.raw().get(0).get(0))) {
            columnList = new ArrayList<ColumnNameType>();
            for (List<String> row : table.raw()) {
                streamColumn = new ColumnNameType(row.get(0),
                        ColumnType.valueOf(row.get(1).toUpperCase()));
                columnList.add(streamColumn);
            }
        }
        try {
            commonspec.getStratioStreamingAPI().createStream(streamName,
                    columnList);
        } catch (Exception e) {
            commonspec.getExceptions().add(e);
            commonspec.getLogger().info(
                    "Caught an exception whilst creating the stream {} : {}",
                    streamName, e);
        }
    }

    @When("^I delete the stream '(.*?)'$")
    public void dropStreamByName(
            @Transform(NullableStringConverter.class) String streamName) {
        commonspec.getLogger().info("Wiping a stream named {}",
                "'" + streamName + "'");

        try {
            commonspec.getStratioStreamingAPI().dropStream(streamName);
        } catch (Exception e) {
            commonspec.getExceptions().add(e);
            commonspec.getLogger().info(
                    "Caught an exception whilst deleting the stream {} : {}",
                    streamName, e);
        }
    }

    @When("^I listen to a stream with name '(.*?)'$")
    public void streamListen(
            @Transform(NullableStringConverter.class) String streamName) {
        commonspec.getLogger().info("Listening to stream {}", streamName);

        try {
            KafkaStream<String, StratioStreamingMessage> listener = commonspec
                    .getStratioStreamingAPI().listenStream(streamName);
            commonspec.setStreamListener(listener);
        } catch (Exception e) {
            commonspec.getExceptions().add(e);
            commonspec
                    .getLogger()
                    .info("Caught an exception whilst listening to the stream {} : {}",
                            streamName, e);
        }
    }

    @When("^I stop listening to a stream with name '(.*?)'$")
    public void streamListenStop(
            @Transform(NullableStringConverter.class) String streamName) {
        commonspec.getLogger().info("Listening to stream {}", streamName);

        try {
            commonspec.getStratioStreamingAPI().stopListenStream(streamName);
        } catch (Exception e) {
            commonspec.getExceptions().add(e);
            commonspec
                    .getLogger()
                    .info("Caught an exception whilst stopping listening to the stream {} : {}",
                            streamName, e);
        }
    }

    @When("^I alter a stream with name '(.*?)', setting its columns \\(with type\\) as:$")
    public void streamAlter(
            @Transform(NullableStringConverter.class) String streamName,
            DataTable table) {
        commonspec.getLogger().info("Altering stream {}", streamName);

        ColumnNameType streamColumn;
        List<ColumnNameType> columnList = null;
        if (!("").equals(table.raw().get(0).get(0))) {
            columnList = new ArrayList<ColumnNameType>();
            for (List<String> row : table.raw()) {
                ColumnType type;
                try {
                    type = ColumnType.valueOf(row.get(1).toUpperCase());
                } catch (IllegalArgumentException e) {
                    type = null;
                }

                streamColumn = new ColumnNameType(row.get(0), type);
                columnList.add(streamColumn);
            }
        }
        try {
            commonspec.getStratioStreamingAPI().alterStream(streamName,
                    columnList);
        } catch (Exception e) {
            commonspec.getExceptions().add(e);
            commonspec.getLogger().info(
                    "Caught an exception whilst altering the stream {} : {}",
                    streamName, e);
        }
    }

    @When("^I insert into a stream with name '(.*?)' this data:$")
    public void insertData(
            @Transform(NullableStringConverter.class) String streamName,
            DataTable table) {
        commonspec.getLogger().info("Inserting into stream {}", streamName);

        List<ColumnNameValue> streamData = new ArrayList<ColumnNameValue>();
        for (List<String> row : table.raw()) {
            ColumnNameValue columnValue = new ColumnNameValue(row.get(0),
                    row.get(1));
            streamData.add(columnValue);
        }

        try {
            commonspec.getStratioStreamingAPI().insertData(streamName,
                    streamData);
        } catch (Exception e) {
            commonspec.getExceptions().add(e);
            commonspec
                    .getLogger()
                    .info("Caught an exception whilst inserting data into the stream {} : {}",
                            streamName, e);
        }
    }

    @When("^I add a query '(.*?)' to a stream with name '(.*?)'$")
    public void addQuery(
            @Transform(NullableStringConverter.class) String query,
            @Transform(NullableStringConverter.class) String streamName) {
        commonspec.getLogger()
                .info("Adding a query to a stream {}", streamName);

        try {
            commonspec.getStratioStreamingAPI().addQuery(streamName, query);
        } catch (Exception e) {
            commonspec.getExceptions().add(e);
            commonspec
                    .getLogger()
                    .info("Caught an exception whilst adding a query into a stream {} : {}",
                            streamName, e);
        }
    }

    @When("^I remove a query '(.*?)' to a stream with name '(.*?)'$")
    public void removeQuery(
            @Transform(NullableStringConverter.class) String query,
            @Transform(NullableStringConverter.class) String streamName) {
        commonspec.getLogger()
                .info("Adding a query to a stream {}", streamName);

        String queryId = null;
        if (query != null) {
            queryId = "-333";
            List<StratioStream> streams = null;
            try {
                streams = commonspec.getStratioStreamingAPI().listStreams();
            } catch (Exception e1) {
                commonspec
                        .getLogger()
                        .info("Caught an exception whilst listing every stream {} : {}",
                                streamName, e1);
            }
            for (StratioStream stream : streams) {
                if (stream.getStreamName().equals(streamName)) {
                    List<StreamQuery> querys = stream.getQueries();
                    for (StreamQuery q : querys) {
                        if (q.getQuery().equals(query)) {
                            queryId = q.getQueryId();
                        }
                    }
                }
            }
        }

        try {
            commonspec.getStratioStreamingAPI()
                    .removeQuery(streamName, queryId);
        } catch (Exception e) {
            commonspec.getExceptions().add(e);
            commonspec
                    .getLogger()
                    .info("Caught an exception whilst removing a query from a stream {} : {}",
                            streamName, e);
        }
    }

    @When("^I start saving to (.*?) a stream with name '(.*?)'$")
    public void saveToBackend(String backend,
                              @Transform(NullableStringConverter.class) String streamName)
            throws InterruptedException {

        try {
            if ("Cassandra".equals(backend)) {
                commonspec.getStratioStreamingAPI().saveToCassandra(streamName);
            } else if ("MongoDB".equals(backend)) {
                commonspec.getStratioStreamingAPI().saveToMongo(streamName);
            }
        } catch (Exception e) {
            commonspec.getExceptions().add(e);
            commonspec.getLogger().info(
                    "Caught an exception whilst saving to " + backend
                            + " a stream {} : {}", streamName, e);
        }
    }

    @When("^I stop saving to (.*?) a stream with name '(.*?)'$")
    public void stopToDB(String backend,
                         @Transform(NullableStringConverter.class) String streamName)
            throws InterruptedException {

        try {
            if ("Cassandra".equals(backend)) {
                commonspec.getStratioStreamingAPI().stopSaveToCassandra(
                        streamName);
            } else if ("MongoDB".equals(backend)) {
                commonspec.getStratioStreamingAPI().stopSaveToMongo(streamName);
            }
        } catch (Exception e) {
            commonspec.getExceptions().add(e);
            commonspec.getLogger().info(
                    "Caught an exception whilst stopping saving to " + backend
                            + " a stream {} : {}", streamName, e);
        }
    }

    @When("^I set a '(.*?)' milliseconds ACK timeout$")
    public void setAckTimeout(
            @Transform(NullableIntegerConverter.class) Integer timeout)
            throws InterruptedException {
        try {
            commonspec.getStratioStreamingAPI().defineAcknowledgeTimeOut(
                    timeout);
        } catch (Exception e) {
            commonspec.getExceptions().add(e);
            commonspec
                    .getLogger()
                    .info("Caught an exception whilst setting {} as ACK timeout : {}",
                            timeout, e);
        }
    }

    @When("^I index a stream with name '(.*?)'$")
    public void doIndex(
            @Transform(NullableStringConverter.class) String streamName)
            throws InterruptedException {
        try {
            commonspec.getStratioStreamingAPI().indexStream(streamName);
        } catch (Exception e) {
            commonspec.getExceptions().add(e);
            commonspec.getLogger().info(
                    "Caught an exception whilst indexing a stream {} : {}",
                    streamName, e);
        }
    }

    @When("^I stop indexing a stream with name '(.*?)'$")
    public void doStopIndex(
            @Transform(NullableStringConverter.class) String streamName)
            throws InterruptedException {
        try {
            commonspec.getStratioStreamingAPI().stopIndexStream(streamName);
        } catch (Exception e) {
            commonspec.getExceptions().add(e);
            commonspec
                    .getLogger()
                    .info("Caught an exception whilst stopping indexing to Mongo a stream {} : {}",
                            streamName, e);
        }
    }

    @When("^I type '(.*?)' to the streaming shell$")
    public void shellCmd(String command) throws InterruptedException {
        commonspec.getLogger().info("Trying to run {} at a streaming shell",
                command);

        try {
            commonspec.getShellIface().sendLine(command);
        } catch (Exception e) {
            commonspec
                    .getLogger()
                    .error("Got exception whilst executing a command at a Streaming shell",
                            e);
        }
        Thread.sleep(10);
    }

    @When("^I close the StreamingAPI session$")
    public void apiClose() {
        commonspec.getLogger().info(
                "Trying to destroy the streaming api channel");
        commonspec.setStratioStreamingAPI(null);
    }

    @When("^I start a new StreamingAPI session$")
    public void apiStart() {
        commonspec.getLogger().info(
                "Trying to start a new streaming api channel");
        try {
            commonspec.getLogger().info(
                    "Starting Stratio Streaming factory on {}:{}, {}:{}",
                    commonspec.getKAFKA_HOST(), commonspec.getKAFKA_PORT(),
                    commonspec.getZOOKEEPER_HOST(),
                    commonspec.getZOOKEEPER_PORT());
            IStratioStreamingAPI stratioStreamingAPI = StratioStreamingAPIFactory
                    .create().initializeWithServerConfig(
                            commonspec.getKAFKA_HOST(),
                            commonspec.getKAFKA_PORT(),
                            commonspec.getZOOKEEPER_HOST(),
                            commonspec.getZOOKEEPER_PORT(),
                            commonspec.getZOOKEEPER_PATH()
                    );
            commonspec.setStratioStreamingAPI(stratioStreamingAPI);

        } catch (Exception e) {
            commonspec.getLogger().error(
                    "Got Exception on connecting to Stratio Streaming", e);
            fail("Unable to create Stratio Streaming Factory");
        }
    }

}