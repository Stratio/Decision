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
package com.stratio.decision.streams;

import java.io.Serializable;

public class QueryDTO implements Serializable {

    private static final long serialVersionUID = -4566524384334289720L;

    private final String queryRaw;

    public QueryDTO(String queryRaw) {
        this.queryRaw = queryRaw;
    }

    public String getQueryRaw() {
        return queryRaw;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null) {
            if (obj instanceof QueryDTO) {
                if (this.hashCode() == obj.hashCode()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        if (queryRaw != null) {
            return queryRaw.trim().replaceAll(" ", "").hashCode();
        } else {
            return 0;
        }
    }
}
