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
package com.stratio.decision.model;

import com.stratio.decision.streams.StreamStatusDTO;

import java.io.*;
import java.util.Arrays;
import java.util.Map;

public class FailoverPersistenceStoreModel implements Serializable {

    private static final long serialVersionUID = 6169852275702710086L;

    private final Map<String, StreamStatusDTO> streamStatuses;
    private final byte[] siddhiSnapshot;

    public FailoverPersistenceStoreModel(Map<String, StreamStatusDTO> streamStatuses, byte[] siddhiSnapshot) {
        this.streamStatuses = streamStatuses;
//        this.siddhiSnapshot = siddhiSnapshot.clone();
        this.siddhiSnapshot = null;
    }

    public FailoverPersistenceStoreModel(byte[] bytes) {
        FailoverPersistenceStoreModel failoverPersistenceStoreModel = ByteToFailOverPersistenceModel(bytes);
        this.streamStatuses = failoverPersistenceStoreModel.getStreamStatuses();
        this.siddhiSnapshot = failoverPersistenceStoreModel.getSiddhiSnapshot();
    }

    public Map<String, StreamStatusDTO> getStreamStatuses() {
        return streamStatuses;
    }

    public byte[] getSiddhiSnapshot() {
        return siddhiSnapshot;
    }

    public byte[] FailOverPersistenceModelToByte() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        byte[] result = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this);
            result = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
            return result;
        }
    }

    private FailoverPersistenceStoreModel ByteToFailOverPersistenceModel(byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = null;
        FailoverPersistenceStoreModel result = null;
        try {
            in = new ObjectInputStream(bis);
            result = (FailoverPersistenceStoreModel)in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bis.close();
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
            return result;
        }
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
        FailoverPersistenceStoreModel other = (FailoverPersistenceStoreModel) obj;
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
        return "FailoverPersistenceStoreModel [streamStatuses=" + streamStatuses + ", siddhiSnapshot="
                + Arrays.toString(siddhiSnapshot) + "]";
    }

}
