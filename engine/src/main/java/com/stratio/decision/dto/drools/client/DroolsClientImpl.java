package com.stratio.decision.dto.drools.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.drools.core.io.impl.UrlResource;
import org.kie.api.KieServices;
import org.kie.api.builder.KieModule;
import org.kie.api.builder.KieRepository;
import org.kie.api.io.Resource;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.QueryResults;
import org.kie.api.runtime.rule.QueryResultsRow;

import com.stratio.decision.dto.drools.configuration.model.DroolsConfiguration;
import com.stratio.decision.dto.drools.configuration.model.DroolsConfigurationGroup;

/**
 * Created by jmartinmenor on 13/10/15.
 */
public class DroolsClientImpl<T> implements DroolsClient<T> {

    private static String AUTH_ENABLED = "enabled";

    private KieSession session;
    private DroolsConfiguration conf;
    private DroolsConfigurationGroup group;

    public DroolsClientImpl(DroolsConfiguration dc, String group) throws IOException {

        this.conf = dc;
        this.group = dc.getGroup(group);

        if(this.group.getUrlWorkBench() != null && !this.group.getUrlWorkBench().equals("")){
            loadWorkBenchSession();
        }else{
            loadLocalSession();
        }

    }

    private void loadLocalSession() {

        KieServices ks = KieServices.Factory.get();
        KieContainer kContainer = ks.getKieClasspathContainer();

        this.session = kContainer.newKieSession(this.group.getSessionName());

    }

    private void loadWorkBenchSession() throws IOException {

       //
       // String url = conf.getUrlBase() + "/" + this.group.getUrlWorkBench();

        List<String> urls = this.group.getUrlWorkBench();

        KieServices ks = KieServices.Factory.get();
        KieRepository kr = ks.getRepository();

        Resource[] aRes = new Resource[urls.size()-1];
        Resource res0 = null;

        for(int i=0 ; i<urls.size() ; i++){


            UrlResource urlResource = (UrlResource) ks.getResources()
                    .newUrlResource(conf.getUrlBase() + "/" + urls.get(i));
            urlResource.setUsername(conf.getUserName());
            urlResource.setPassword(conf.getUserPass());
            urlResource.setBasicAuthentication(AUTH_ENABLED);

            InputStream is = urlResource.getInputStream();
            if(i==0)
                res0 = ks.getResources().newInputStreamResource(is);
            else
                aRes[i-1] = ks.getResources().newInputStreamResource(is);

        }


        KieModule kModule = kr.addKieModule(res0,aRes);


        KieContainer kContainer = ks.newKieContainer(kModule.getReleaseId());
        this.session = kContainer.newKieSession(this.group.getSessionName());

    }

    public List fireRules(List<Object> dataList) {

        List res = new ArrayList<T>();

        for(Object data : dataList){
            session.insert(data);
        }

        session.fireAllRules();



        QueryResults queryResults = session.getQueryResults(this.group.getQueryName());
        Iterator<QueryResultsRow> rows = queryResults.iterator();
        while (rows.hasNext()) {
            QueryResultsRow row = rows.next();
            res.add( row.get(this.group.getQueryResultName()));
        }


        Collection<FactHandle> facts = session.getFactHandles();

        FactHandle[] factArray = facts.toArray(new FactHandle[facts.size()]);
        for (int cont=0;cont<factArray.length;cont++) {
            session.delete(factArray[cont]);
        }


        return res;
    }


}
