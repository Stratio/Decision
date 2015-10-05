package com.stratio.decision.shell.wrapper;

import com.stratio.decision.api.IStratioStreamingAPI;
import com.stratio.decision.commons.exceptions.StratioEngineConnectionException;

/**
 * Created by ajnavarro on 24/11/14.
 */
public class StratioStreamingApiWrapper {
    private final IStratioStreamingAPI stratioStreamingAPI;

    public StratioStreamingApiWrapper(IStratioStreamingAPI stratioStreamingAPI) {
        this.stratioStreamingAPI = stratioStreamingAPI;
    }

    public IStratioStreamingAPI api() throws StratioEngineConnectionException {
        if (!stratioStreamingAPI.isInit()) {
            return stratioStreamingAPI.init();
        } else {
            return stratioStreamingAPI;
        }
    }

}
