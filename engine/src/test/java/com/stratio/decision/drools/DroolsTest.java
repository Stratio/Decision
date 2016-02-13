package com.stratio.decision.drools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.stratio.decision.configuration.ConfigurationContext;
import com.stratio.decision.drools.sessions.Results;

import junit.framework.Assert;

/**
 * Created by jmartinmenor on 15/01/16.
 */

public class DroolsTest {

    private DroolsConnectionContainer dcon;


    @Before
    public void setup(){
        ConfigurationContext cc = new ConfigurationContext();

        this.dcon = new DroolsConnectionContainer(cc.getDroolsConfiguration());
    }

    @Test
    public void testSatefullSession(){

        DroolsInstace container = dcon.getGroupContainer("statefull");

        List data = new ArrayList();
        Map m = new HashMap<String,Object>();
        m.put("col1",1);
        m.put("col2",1);
        data.add(m);

        Results results = container.getSession().fireRules(data);

        Assert.assertNotNull(results.getResults().get(0));

    }

    @Test
    @Ignore
    public void testStelessSession(){

        DroolsInstace container = dcon.getGroupContainer("stateless");

        List data = new ArrayList();
        Map m = new HashMap<String,Object>();
        m.put("col1",1);
        m.put("col2",1);
        data.add(m);

        Results results = container.getSession().fireRules(data);

        Assert.assertNotNull(results.getResults().get(0));
    }
}
