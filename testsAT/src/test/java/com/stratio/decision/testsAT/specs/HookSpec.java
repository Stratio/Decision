package com.stratio.decision.testsAT.specs;

import static net.sf.expectit.filter.Filters.removeColors;
import static net.sf.expectit.filter.Filters.removeNonPrintable;
import static net.sf.expectit.matcher.Matchers.eof;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.stratio.decision.api.IStratioStreamingAPI;
import com.stratio.decision.api.StratioStreamingAPIFactory;
import com.stratio.decision.commons.exceptions.StratioStreamingException;

import cucumber.api.java.After;
import cucumber.api.java.Before;
import net.sf.expectit.Expect;
import net.sf.expectit.ExpectBuilder;

public class HookSpec extends BaseSpec {

    public HookSpec() {
    }

    public HookSpec(Common spec) {
        this.commonspec = spec;
    }

    @Before(order = 20, value = "@MongoDB")
    public void mongoDBWipe() throws IOException, InterruptedException {
        commonspec.getLogger().info("Wiping every Mongo collection under stratiostreaming database (if it does exist)");
        commonspec.getMongoDBClient().connectToMongoDBDataBase("stratiostreaming");
        Set<String> collections = commonspec.getMongoDBClient().getMongoDBCollections();
        for (String collection : collections) {
            commonspec.getMongoDBClient().dropAllDataMongoDBCollection(collection);
        }
    }

    @Before(order = 20, value = "@C*")
    public void cassandraKSWipe() throws IOException, InterruptedException {
        commonspec.getLogger().info("Wiping every C* table under stratio_streaming keyspace (if it does exist)");
        List<String> tables = commonspec.getCassandraClient().getTables("stratio_streaming");
        for (String table : tables) {
            commonspec.getCassandraClient().executeQuery("TRUNCATE stratio_streaming.\"" + table + "\" ;");
        }
    }

    @Before(order = 10, value = "@shell")
    public void shellSetup() throws IOException, InterruptedException {

        commonspec.getLogger().info("Stratio Streaming shell setup");
        String streamingHome = System.getProperty("STRATIO-STREAMING_SHELL_HOME", "../stratio-streaming/");

        File shell = new File(streamingHome);

        ProcessBuilder pb = new ProcessBuilder(shell.getAbsolutePath());
        Map<String, String> env = pb.environment();
        env.put("SHELL_OPTS", System.getProperty("SHELL_OPTS", ""));

        Process process = pb.start();

        Expect expect = new ExpectBuilder().withInputs(process.getInputStream())
                .withInputFilters(removeNonPrintable(), removeColors()).withOutput(process.getOutputStream())
                        // .withEchoInput(System.out).withEchoOutput(System.out)
                .withTimeout(20, TimeUnit.SECONDS)
                        // .withErrorOnTimeout(true)
                .build();

        Thread.sleep(1000);

        commonspec.setShellIface(expect);
    }

    @Before(order = 10, value = "@api")
    public void kafkaSetup() {
        commonspec.setKAFKA_HOST(System.getProperty("KAFKA_HOST", "127.0.0.1"));
        commonspec.setKAFKA_PORT(Integer.parseInt(System.getProperty("KAFKA_PORT", "9092")));
        commonspec.setZOOKEEPER_HOST(System.getProperty("ZOOKEEPER_HOST", "127.0.0.1"));
        commonspec.setZOOKEEPER_PORT(Integer.parseInt(System.getProperty("ZOOKEEPER_PORT", "2181")));
    }

    @Before(order = 20, value = "@api")
    public void streamingApiSetup() {
        commonspec.getLogger().info("Stratio Streaming API setup");
        try {
            commonspec.getLogger().info("Starting Stratio Streaming factory on {}:{}, {}:{}",
                    commonspec.getKAFKA_HOST(), commonspec.getKAFKA_PORT(), commonspec.getZOOKEEPER_HOST(),
                    commonspec.getZOOKEEPER_PORT());
            IStratioStreamingAPI stratioStreamingAPI = StratioStreamingAPIFactory.create().initializeWithServerConfig(
                    commonspec.getKAFKA_HOST(), commonspec.getKAFKA_PORT(), commonspec.getZOOKEEPER_HOST(),
                    commonspec.getZOOKEEPER_PORT());
            commonspec.setStratioStreamingAPI(stratioStreamingAPI);

        } catch (StratioStreamingException e) {
            commonspec.getLogger().error("Got Exception on connecting to Stratio Streaming", e);
            fail("Unable to create Stratio Streaming Factory");
        }
    }

    @After(order = 20, value = "@api")
    public void apiTeardown() throws IOException {
        commonspec.getLogger().info("Clearing Stratio Streaming API");
        if (commonspec.getStratioStreamingAPI() != null) {
            commonspec.getStratioStreamingAPI().close();
        }

    }

    @After(order = 20, value = "@shell")
    public void shellTeardown() throws IOException {
        commonspec.getLogger().info("Closing Stratio Streaming shell prompt");
        if (commonspec.getShellIface() != null) {
            commonspec.getShellIface().sendLine("exit");
            commonspec.getShellIface().expect(eof());
            commonspec.getShellIface().close();
        }
    }
}