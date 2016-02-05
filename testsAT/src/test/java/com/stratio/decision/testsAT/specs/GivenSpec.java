package com.stratio.decision.testsAT.specs;

import java.util.List;

import com.stratio.decision.commons.exceptions.StratioAPIGenericException;
import com.stratio.decision.commons.exceptions.StratioAPISecurityException;
import com.stratio.decision.commons.exceptions.StratioEngineOperationException;
import com.stratio.decision.commons.exceptions.StratioEngineStatusException;
import com.stratio.decision.commons.streams.StratioStream;

import cucumber.api.java.en.Given;

public class GivenSpec extends BaseSpec {

    public GivenSpec(Common spec) {
        this.commonspec = spec;
    }

    @Given("^I drop every existing stream$")
    public void dropStreams() throws StratioEngineStatusException,
            StratioAPISecurityException, StratioEngineOperationException,
            StratioAPIGenericException {
        commonspec.getLogger().info("Wiping every stream");
        List<StratioStream> streamList = commonspec.getStratioStreamingAPI()
                .listStreams();

        for (StratioStream stream : streamList) {
            if (stream.getUserDefined()) {
                commonspec.getLogger()
                        .info("Wiping {}", stream.getStreamName());
                String streamName = stream.getStreamName();
                commonspec.getStratioStreamingAPI().dropStream(streamName);
            } else {
                commonspec.getLogger().info(
                        "Unabe to wipe {}: internal stream",
                        stream.getStreamName());
            }
        }
    }
}