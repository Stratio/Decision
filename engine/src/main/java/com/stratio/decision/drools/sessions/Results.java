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
