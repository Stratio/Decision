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
