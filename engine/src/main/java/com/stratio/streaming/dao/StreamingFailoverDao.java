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
package com.stratio.streaming.dao;

import java.util.Calendar;

import com.google.gson.Gson;
import com.stratio.streaming.configuration.ConfigurationContext;
import com.stratio.streaming.utils.ZKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.streaming.model.FailoverPersistenceStoreModel;

public class StreamingFailoverDao  {

    private static final Logger log = LoggerFactory.getLogger(StreamingFailoverDao.class);

    private static final String PERSISTENCE_STORE_PATH = "/failoverStorage";

    private ZKUtils zkutils;

    private Gson gson;

    public StreamingFailoverDao(ConfigurationContext configurationContext, Gson gson) throws Exception {
        this.gson = gson;
        this.zkutils = ZKUtils.getZKUtils(configurationContext.getZookeeperHostsQuorum());
    }


    public FailoverPersistenceStoreModel load() throws Exception {
        if (zkutils.existZNode(PERSISTENCE_STORE_PATH)) {
            log.info("Failover loading data...");
            byte[] bytes = zkutils.getZNode(PERSISTENCE_STORE_PATH);
            return gson.fromJson(new String(bytes), FailoverPersistenceStoreModel.class);
//            return new FailoverPersistenceStoreModel(bytes);
        } else {
            return null;
        }
    }

    public void save(FailoverPersistenceStoreModel failoverPersistenceStoreModel) throws Exception {
        log.info("Failover data to save. HASH {}, TOSTRING {} ", failoverPersistenceStoreModel.hashCode(),
                failoverPersistenceStoreModel);
        zkutils.createZNode(PERSISTENCE_STORE_PATH, gson.toJson(failoverPersistenceStoreModel).getBytes());
//        zkutils.createZNode(PERSISTENCE_STORE_PATH, failoverPersistenceStoreModel.FailOverPersistenceModelToByte());
    }

}
