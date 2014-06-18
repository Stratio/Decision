package com.stratio.streaming.streams;

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
