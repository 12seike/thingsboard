package org.thingsboard.server.dao.timeseries.tdengine;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.*;
import org.thingsboard.server.dao.timeseries.TimeseriesLatestDao;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;

@Component
@Primary
@Slf4j
public class TdEngineTimeseriesLatestDao implements TimeseriesLatestDao {

    private static final String JDBC_URL = "jdbc:TAOS-RS://localhost:6041/thingsboard?user=root&password=taosdata";
    private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));

    @Override
    public ListenableFuture<Optional<TsKvEntry>> findLatestOpt(TenantId tenantId, EntityId entityId, String key) {
        return executor.submit(() -> {
            try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
                String sql = "SELECT ts, val FROM ts_kv_latest_stable " +
                        "WHERE tenant_id = ? AND entity_type = ? AND entity_id = ? AND key_name = ?";
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setString(1, tenantId.getId().toString());
                    stmt.setString(2, entityId.getEntityType().name());
                    stmt.setString(3, entityId.getId().toString());
                    stmt.setString(4, key);
                    ResultSet rs = stmt.executeQuery();
                    if (rs.next()) {
                        long ts = rs.getTimestamp("ts").getTime();
                        String val = rs.getString("val");
                        return Optional.of(new BasicTsKvEntry(ts, new StringDataEntry(key, val)));
                    }
                }
            } catch (Exception e) {
                log.error("findLatestOpt error", e);
            }
            return Optional.empty();
        });
    }

    @Override
    public ListenableFuture<TsKvEntry> findLatest(TenantId tenantId, EntityId entityId, String key) {
        return Futures.immediateFuture(null);
    }

    @Override
    public ListenableFuture<List<TsKvEntry>> findAllLatest(TenantId tenantId, EntityId entityId) {
        if (!"DEVICE".equals(entityId.getEntityType().name())) {
            return Futures.immediateFuture(Collections.emptyList());
        }

        return executor.submit(() -> {
            List<TsKvEntry> result = new ArrayList<>();
            try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
                String sql = "SELECT key_name, ts, val FROM ts_kv_latest_stable "
                        + "WHERE tenant_id = ? AND entity_type = ? AND entity_id = ?";
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setString(1, tenantId.getId().toString());
                    stmt.setString(2, "DEVICE");
                    stmt.setString(3, entityId.getId().toString());

                    ResultSet rs = stmt.executeQuery();
                    while (rs.next()) {
                        String key = rs.getString("key_name");
                        long ts = rs.getTimestamp("ts").getTime();
                        String val = rs.getString("val");
                        result.add(new BasicTsKvEntry(ts, new StringDataEntry(key, val)));
                        log.info("🟢 查询到设备数据：{} = {}", key, val);
                    }
                }
            } catch (Exception e) {
                log.error("查询失败", e);
            }
            log.info("✅ 页面查询返回条数：{}", result.size());
            return result;
        });
    }

    @Override
    public ListenableFuture<Long> saveLatest(TenantId tenantId, EntityId entityId, TsKvEntry tsKvEntry) {
        if (!"DEVICE".equals(entityId.getEntityType().name())) {
            return Futures.immediateFuture(0L);
        }

        return executor.submit(() -> {
            try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
                String sql = "INSERT INTO ts_kv_latest USING ts_kv_latest_stable " +
                        "TAGS(?,?,?,?) VALUES(?,?)";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setString(1, tenantId.getId().toString());
                    pstmt.setString(2, "DEVICE");
                    pstmt.setString(3, entityId.getId().toString());
                    pstmt.setString(4, tsKvEntry.getKey());
                    pstmt.setTimestamp(5, new Timestamp(tsKvEntry.getTs()));
                    pstmt.setString(6, tsKvEntry.getValueAsString());
                    pstmt.executeUpdate();
                    log.info("✅ 设备写入成功：{} = {}", tsKvEntry.getKey(), tsKvEntry.getValueAsString());
                }
            } catch (Exception e) {
                log.error("设备写入失败", e);
            }
            return 1L;
        });
    }

    @Override
    public ListenableFuture<TsKvLatestRemovingResult> removeLatest(TenantId tenantId, EntityId entityId, DeleteTsKvQuery query) {
        return Futures.immediateFuture(null);
    }

    @Override
    public List<String> findAllKeysByDeviceProfileId(TenantId tenantId, DeviceProfileId deviceProfileId) {
        return Collections.emptyList();
    }

    @Override
    public List<String> findAllKeysByEntityIds(TenantId tenantId, List<EntityId> entityIds) {
        return Collections.emptyList();
    }
}