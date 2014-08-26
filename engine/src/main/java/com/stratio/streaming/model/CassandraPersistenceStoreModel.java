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
package com.stratio.streaming.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

import com.stratio.streaming.streams.StreamStatusDTO;

public class CassandraPersistenceStoreModel implements Serializable {

    private static final long serialVersionUID = 6169852275702710086L;

    private final Map<String, StreamStatusDTO> streamStatuses;
    private final byte[] siddhiSnapshot;

    public CassandraPersistenceStoreModel(Map<String, StreamStatusDTO> streamStatuses, byte[] siddhiSnapshot) {
        this.streamStatuses = streamStatuses;
        this.siddhiSnapshot = siddhiSnapshot;
    }

    public Map<String, StreamStatusDTO> getStreamStatuses() {
        return streamStatuses;
    }

    public byte[] getSiddhiSnapshot() {
        return siddhiSnapshot;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(siddhiSnapshot);
        result = prime * result + ((streamStatuses == null) ? 0 : streamStatuses.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CassandraPersistenceStoreModel other = (CassandraPersistenceStoreModel) obj;
        if (!Arrays.equals(siddhiSnapshot, other.siddhiSnapshot))
            return false;
        if (streamStatuses == null) {
            if (other.streamStatuses != null)
                return false;
        } else if (!streamStatuses.equals(other.streamStatuses))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "CassandraPersistenceStoreModel [streamStatuses=" + streamStatuses + ", siddhiSnapshot="
                + Arrays.toString(siddhiSnapshot) + "]";
    }

}
