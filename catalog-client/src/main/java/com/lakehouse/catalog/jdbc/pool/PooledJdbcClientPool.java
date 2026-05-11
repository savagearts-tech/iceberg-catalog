package com.lakehouse.catalog.jdbc.pool;

import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.jdbc.UncheckedSQLException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class PooledJdbcClientPool extends JdbcClientPool {

    private final JdbcConnectionPoolHandle poolHandle;

    public PooledJdbcClientPool(
            String jdbcUrl,
            Map<String, String> properties,
            JdbcConnectionPoolHandle poolHandle
    ) {
        super(JdbcPoolProperties.configuredMaxSize(properties), jdbcUrl, properties);
        this.poolHandle = poolHandle;
    }

    @Override
    protected Connection newClient() {
        try {
            return poolHandle.dataSource().getConnection();
        } catch (SQLException exception) {
            throw new UncheckedSQLException(
                    exception,
                    "Failed to connect using JDBC pool provider: %s",
                    poolHandle.providerName());
        }
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            poolHandle.close();
        }
    }
}
