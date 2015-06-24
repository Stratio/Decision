/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.streaming.functions;

import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

import com.stratio.streaming.service.SolrOperationsService;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SaveToSolrActionExecutionFunction extends BaseActionExecutionFunction {

    private static final long serialVersionUID = 3522740757019463301L;

    private static final Logger log = LoggerFactory.getLogger(SaveToSolrActionExecutionFunction.class);

    private Map<String, SolrClient> solrClients = new HashMap<>();

    private final String solrHosts;
    private final Boolean isCloud;

    public SaveToSolrActionExecutionFunction(String solrHosts, Boolean isCloud) {
        this.solrHosts = solrHosts;
        this.isCloud = isCloud;
    }

    @Override
    public void process(Iterable<StratioStreamingMessage> messages) throws Exception {

        for (StratioStreamingMessage stratioStreamingMessage : messages) {
            SolrInputDocument document = new SolrInputDocument();
            for (ColumnNameTypeValue column : stratioStreamingMessage.getColumns()) {
                document.addField(column.getColumn(), column.getValue());
            }
            getClient(stratioStreamingMessage.getStreamName()).add(document);
        }
        flushClients();
    }

    private SolrClient getClient(String core) {
        if (solrClients.containsKey(core)) {
            return solrClients.get(core);
        } else {
            SolrClient solrClient;
            if (isCloud) {
                solrClient = new CloudSolrClient("http://" + solrHosts + "/solr/" + core);
            } else {
                solrClient = new HttpSolrClient("http://" + solrHosts + "/solr/" + core);
            }
            solrClients.put(core, solrClient);
            return solrClient;
        }
    }

    private void flushClients() throws IOException, SolrServerException {
        //Do commit in all Solrclients
        for (String core : solrClients.keySet()) {
            getClient(core).commit();
        }
    }


}
