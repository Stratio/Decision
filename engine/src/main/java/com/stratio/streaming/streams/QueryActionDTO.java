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
package com.stratio.streaming.streams;

import com.stratio.streaming.commons.constants.StreamAction;

public class QueryActionDTO {

    private StreamAction streamAction;
    private String actionQueryId;

    public QueryActionDTO() {
    }

    public QueryActionDTO(StreamAction streamAction, String actionQueryId) {
        this.streamAction = streamAction;
        this.actionQueryId = actionQueryId;
    }

    @Override
    public boolean equals(Object obj) {
        return streamAction.equals(obj);
    }

    public StreamAction getStreamAction() {
        return streamAction;
    }

    public String getActionQueryId() {
        return actionQueryId;
    }

}
