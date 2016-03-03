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
package com.stratio.decision.unit.siddhi.query;

import com.stratio.decision.siddhi.extension.DistinctWindowExtension;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by aitor on 9/16/15.
 */
public abstract class SiddhiQueryHelper {

    public static SiddhiManager sm;

    public static AtomicInteger count;

    public static String STREAM_NAME= "streamBase";
    public static String STREAM_RESULT= "resultStream";

    public static SiddhiManager initializeSiddhiManager() {
        SiddhiConfiguration config = new SiddhiConfiguration();
        @SuppressWarnings("rawtypes")
        List<Class> extensions = new ArrayList<Class>();
        extensions.add(DistinctWindowExtension.class);
        config.setSiddhiExtensions(extensions);
        sm = new SiddhiManager(config);
        return sm;
    }

    public static void shutdownSiddhiManager()    {
        sm.shutdown();
    }
}
