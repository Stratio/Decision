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
package com.stratio.streaming.service;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.Attribute;

public class StreamMetadataService {

    private final SiddhiManager siddhiManager;

    public StreamMetadataService(SiddhiManager siddhiManager) {
        this.siddhiManager = siddhiManager;
    }

    public int getAttributePosition(String streamName, String columnName) {
        return siddhiManager.getStreamDefinition(streamName).getAttributePosition(columnName);
    }

    public Attribute getAttribute(String streamName, int columnOrder) {
        return siddhiManager.getStreamDefinition(streamName).getAttributeList().get(columnOrder);
    }

    public byte[] getSnapshot() {
        return siddhiManager.snapshot();
    }

    public void setSnapshot(byte[] snapshot) {
        siddhiManager.restore(snapshot);
    }
}
