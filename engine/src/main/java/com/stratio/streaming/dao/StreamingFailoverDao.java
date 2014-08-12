package com.stratio.streaming.dao;

import java.util.Calendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.google.gson.Gson;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.model.CassandraPersistenceStoreModel;

public class StreamingFailoverDao {

    private static final Logger log = LoggerFactory.getLogger(StreamingFailoverDao.class);

    private static final String PERSISTENCE_STORE_TABLENAME = "failoverStorage";

    private static final int toYear = 2010;

    private final Session session;

    private final Gson gson;

    public StreamingFailoverDao(Session session, Gson gson) {
        this.session = session;
        this.gson = gson;
        init();
    }

    public CassandraPersistenceStoreModel load() {
        for (int i = Calendar.getInstance().get(Calendar.YEAR); i >= toYear; i--) {
            RegularStatement statement = QueryBuilder.select("data")
                    .from(STREAMING.STREAMING_KEYSPACE_NAME, PERSISTENCE_STORE_TABLENAME)
                    .where(QueryBuilder.eq("year", i)).orderBy(QueryBuilder.desc("timestamp")).limit(1);

            log.info(statement.getQueryString());

            ResultSet resultSet = session.execute(statement);

            Row row = resultSet.one();
            if (row != null) {
                String dataString = row.getString("data");
                CassandraPersistenceStoreModel result = gson.fromJson(dataString, CassandraPersistenceStoreModel.class);
                log.info("Failover data loaded. HASH {}, TOSTRING {} ", result.hashCode(), result);
                return result;
            }

        }
        return null;
    }

    public void save(CassandraPersistenceStoreModel cassandraPersistenceStoreModel) {
        log.info("Failover data to save. HASH {}, TOSTRING {} ", cassandraPersistenceStoreModel.hashCode(),
                cassandraPersistenceStoreModel);
        session.execute(QueryBuilder.insertInto(STREAMING.STREAMING_KEYSPACE_NAME, PERSISTENCE_STORE_TABLENAME)
                .value("timestamp", UUIDs.timeBased()).value("data", gson.toJson(cassandraPersistenceStoreModel))
                .value("year", Calendar.getInstance().get(Calendar.YEAR)));
    }

    private void init() {
        // XXX future refactor. code repeated.
        session.execute(String
                .format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                        STREAMING.STREAMING_KEYSPACE_NAME));
        session.execute(String
                .format("CREATE TABLE IF NOT EXISTS %s.%s (timestamp timeuuid,year int, data text, PRIMARY KEY (year, timestamp)) WITH compression = {'sstable_compression': ''}",
                        STREAMING.STREAMING_KEYSPACE_NAME, PERSISTENCE_STORE_TABLENAME));
    }
}
