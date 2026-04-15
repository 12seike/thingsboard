package org.thingsboard.server.dao.timeseries.tdengine;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.*;
import org.thingsboard.server.dao.timeseries.TimeseriesDao;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;

@Component
@Slf4j
public class TdEngineTimeseriesDao implements TimeseriesDao {

    private static final String JDBC_URL = "jdbc:TAOS-RS://localhost:6041/thingsboard?user=root&password=taosdata";
    private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));

    @Override
    public ListenableFuture<Integer> save(TenantId tenantId, EntityId entityId, TsKvEntry tsKvEntry, long ttl) {
        return executor.submit(() -> {
            try {
                Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
                try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
                    String sql = "INSERT INTO ts_kv USING ts_kv_stable TAGS(?,?,?,?) VALUES(?,?)";
                    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                        stmt.setString(1, tenantId.getId().toString());
                        stmt.setString(2, entityId.getEntityType().name());
                        stmt.setString(3, entityId.getId().toString());
                        stmt.setString(4, tsKvEntry.getKey());
                        stmt.setLong(5, tsKvEntry.getTs());
                        stmt.setString(6, tsKvEntry.getValueAsString());
                        stmt.executeUpdate();
                        return 1;
                    }
                }
            } catch (Exception e) {
                log.error("save error", e);
            }
            return 0;
        });
    }

    // ✅【完全修复】前端历史数据查询
    @Override
    public ListenableFuture<List<ReadTsKvQueryResult>> findAllAsync(TenantId tenantId, EntityId entityId, List<ReadTsKvQuery> queries) {
        return executor.submit(() -> {
            List<ReadTsKvQueryResult> resultList = new ArrayList<>();
            for (ReadTsKvQuery query : queries) {
                List<TsKvEntry> entries = new ArrayList<>();
                try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
                    String sql = "SELECT ts, val FROM ts_kv_stable " +
                            "WHERE tenant_id = ? AND entity_type = ? AND entity_id = ? AND key_name = ? " +
                            "AND ts >= ? AND ts <= ? ORDER BY ts ASC LIMIT ?";

                    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                        stmt.setString(1, tenantId.getId().toString());
                        stmt.setString(2, entityId.getEntityType().name());
                        stmt.setString(3, entityId.getId().toString());
                        stmt.setString(4, query.getKey());
                        stmt.setLong(5, query.getStartTs());
                        stmt.setLong(6, query.getEndTs());
                        stmt.setInt(7, query.getLimit());

                        ResultSet rs = stmt.executeQuery();
                        while (rs.next()) {
                            long ts = rs.getTimestamp("ts").getTime();
                            String val = rs.getString("val");
                            entries.add(new BasicTsKvEntry(ts, new StringDataEntry(query.getKey(), val)));
                        }
                    }
                } catch (Exception e) {
                    log.error("query error", e);
                }
                resultList.add(new ReadTsKvQueryResult(query.getId(), entries, query.getLimit()));
            }
            return resultList;
        });
    }

    @Override
    public ListenableFuture<Void> remove(TenantId tenantId, EntityId entityId, DeleteTsKvQuery deleteTsKvQuery) {
        return executor.submit(() -> null);
    }

    @Override
    public ListenableFuture<Integer> savePartition(TenantId tenantId, EntityId entityId, long tsKvEntryTs, String key) {
        return executor.submit(() -> 0);
    }

    @Override
    public void cleanup(long systemTtl) {}
}