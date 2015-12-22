package com.stratio.decision.drools.sessions;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by josepablofernandez on 3/12/15.
 */
public class Results {

    private List results;

    public Results(){

        results = new ArrayList();
    }


    public List getResults() {
        return results;
    }

    public boolean addResults(Object o){
        return results.add(o);
    }

}
