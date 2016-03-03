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
package com.stratio.decision.drools.configuration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by jmartinmenor on 8/10/15.
 */
public class DroolsConfigurationBean {

    private Boolean isEnabled= true;

    private Map<String, DroolsConfigurationGroupBean> groups;

    public Map<String, DroolsConfigurationGroupBean> getGroups() {
        return groups;
    }

    public void setGroups(Map<String, DroolsConfigurationGroupBean> groups) {
        this.groups = groups;
    }

    public String getQueryNameGroup(String group){
        return groups.get(group).getQueryName();
    }

    public List<String> getListGroups(){

        if (groups == null || groups.size() == 0 ){
            return null;
        }
        List<String> keys = new ArrayList<String>();
        Iterator<String> ite = groups.keySet().iterator();
        while(ite.hasNext()){
            keys.add(ite.next());
        }
        return keys;
    }

    public DroolsConfigurationGroupBean getGroup(String group){
        return groups.get(group);
    }

    public Boolean getIsEnabled()   {
        return this.isEnabled;
    }

    public void setIsEnabled(Boolean isEnabled) {
        this.isEnabled= isEnabled;
    }
}
