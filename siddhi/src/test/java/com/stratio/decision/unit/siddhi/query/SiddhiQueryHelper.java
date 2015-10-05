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
