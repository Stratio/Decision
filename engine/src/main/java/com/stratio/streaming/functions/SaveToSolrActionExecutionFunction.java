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
import org.hsqldb.lib.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.Collection;
import java.util.HashMap;

public class SaveToSolrActionExecutionFunction extends BaseActionExecutionFunction {

    private static final long serialVersionUID = 3522740757019463301L;

    private static final Logger log = LoggerFactory.getLogger(SaveToSolrActionExecutionFunction.class);

    private Map<String, SolrClient> solrClients = new HashMap<>();
    private List<String> solrCores = new ArrayList<String>();

    private SolrOperationsService solrOperationsService;

    private final String dataDir;
    private final String solrHosts;
    private final Boolean isCloud;

    public SaveToSolrActionExecutionFunction(String solrHosts, Boolean isCloud, String dataDir) {
        this.solrHosts = solrHosts;
        this.dataDir = dataDir;
        this.isCloud = isCloud;
        this.solrOperationsService = new SolrOperationsService(solrHosts, dataDir, isCloud);
    }

    @Override
    public void process(Iterable<StratioStreamingMessage> messages) throws Exception {
        Map<String, Collection<SolrInputDocument>> elemntsToInsert = new HashMap<String, Collection<SolrInputDocument>>();
        for (StratioStreamingMessage stratioStreamingMessage : messages) {
            SolrInputDocument document = new SolrInputDocument();
            document.addField("id", stratioStreamingMessage.getTimestamp());
            for (ColumnNameTypeValue column : stratioStreamingMessage.getColumns()) {
                document.addField(column.getColumn(), column.getValue());
            }
            checkCore(stratioStreamingMessage);
            Collection<SolrInputDocument> collection = elemntsToInsert.get(stratioStreamingMessage.getStreamName());
            collection.add(document);
            elemntsToInsert.put(stratioStreamingMessage.getStreamName(), collection);
        }
        for (Map.Entry<String, Collection<SolrInputDocument>> elem: elemntsToInsert.entrySet()) {
            getSolrclient(elem.getKey()).add(elem.getValue());
        }
        flushClients();
    }

    private void checkCore(StratioStreamingMessage message) throws IOException, SolrServerException, ParserConfigurationException, TransformerException, SAXException, URISyntaxException {
        String core = message.getStreamName();
        //check if core exists
        if (solrCores.size() == 0) {
            // Initialize solrcores list
            solrCores = solrOperationsService.getCoreList();
        }
        if (!solrCores.contains(core)) {
            // Create Core
            solrOperationsService.createCore(message);
            // Update core list
            solrCores = solrOperationsService.getCoreList();
        }
    }

    private SolrClient getClient(StratioStreamingMessage message) throws IOException, SolrServerException, URISyntaxException, TransformerException, SAXException, ParserConfigurationException {
        String core = message.getStreamName();
        if (solrClients.containsKey(core)) {
            //we have a client for this core
            return solrClients.get(core);
        } else {
            SolrClient solrClient = getSolrclient(core);
            solrClients.put(core, solrClient);
            return solrClient;
        }
    }

    private void flushClients() throws IOException, SolrServerException, URISyntaxException {
        //Do commit in all Solrclients
        for (String core : solrClients.keySet()) {
            getSolrclient(core).commit();
        }
    }

    private SolrClient getSolrclient(String core) {
        SolrClient solrClient;
        if (isCloud) {
            solrClient = new CloudSolrClient(solrHosts);
            ((CloudSolrClient)solrClient).setDefaultCollection(core);
        } else {
            solrClient = new HttpSolrClient("http://" + solrHosts + "/solr/" + core);
        }
        return solrClient;
    }



}
