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
package com.stratio.decision.task;

import com.stratio.decision.service.StreamingFailoverService;

public class FailOverTask implements Runnable {

    private final StreamingFailoverService streamingFailoverService;

    public FailOverTask(StreamingFailoverService streamingFailoverService) throws Exception {
        this.streamingFailoverService = streamingFailoverService;
        streamingFailoverService.load();
    }

    @Override
    public void run() {
        try {
            streamingFailoverService.save();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
