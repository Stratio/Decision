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
        List<StratioStream> streamList = commonspec.getStratioStreamingAPI()
                .listStreams();

        String strStreamList = "";
        if (streamList.size()==1)
        {
            strStreamList = streamList.get(0).getStreamName() + strStreamList;
        } else {
            for (int i = 0; i < streamList.size(); i++) {
                if (i == streamList.size() - 1) {
                    strStreamList = streamList.get(i).getStreamName() + strStreamList;
                } else {
                    strStreamList = strStreamList + ", " +streamList.get(i).getStreamName();
                }
            }
        }
        commonspec.getLogger().info("Wiping every stream [{}]", strStreamList);
        for (StratioStream stream : streamList) {
            if (stream.getUserDefined()) {
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