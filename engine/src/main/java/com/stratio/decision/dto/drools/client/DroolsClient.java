package com.stratio.decision.dto.drools.client;

import java.util.List;

/**
 * Created by jmartinmenor on 13/10/15.
 */
public interface DroolsClient<T>{

    public List<T> fireRules(List<Object> dataList);
}
