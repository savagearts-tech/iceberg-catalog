package com.lakehouse.catalog.jdbc.pool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.util.Map;
import java.util.Properties;

public class HikariJdbcConnectionPoolProvider implements JdbcConnectionPoolProvider {

    @Override
    public String providerName() {
        return "hikari";
    }

    @Override
    public JdbcConnectionPoolHandle create(
            String jdbcUrl,
            Map<String, String> catalogProperties,
            Map<String, String> driverProperties
    ) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setPoolName(catalogProperties.getOrDefault(
                JdbcPoolProperties.POOL_NAME,
                "iceberg-jdbc-" + Integer.toHexString(jdbcUrl.hashCode())));
        config.setMaximumPoolSize(Math.max(1, JdbcPoolProperties.configuredMaxSize(catalogProperties)));
        config.setMinimumIdle(Math.min(
                config.getMaximumPoolSize(),
                Integer.parseInt(catalogProperties.getOrDefault(JdbcPoolProperties.MIN_IDLE, "1"))));
        config.setConnectionTimeout(Long.parseLong(
                catalogProperties.getOrDefault(JdbcPoolProperties.CONNECTION_TIMEOUT_MS, "30000")));
        config.setIdleTimeout(Long.parseLong(
                catalogProperties.getOrDefault(JdbcPoolProperties.IDLE_TIMEOUT_MS, "600000")));
        config.setMaxLifetime(Long.parseLong(
                catalogProperties.getOrDefault(JdbcPoolProperties.MAX_LIFETIME_MS, "1800000")));
        config.setValidationTimeout(Long.parseLong(
                catalogProperties.getOrDefault(JdbcPoolProperties.VALIDATION_TIMEOUT_MS, "5000")));

        long leakDetectionThreshold = Long.parseLong(
                catalogProperties.getOrDefault(JdbcPoolProperties.LEAK_DETECTION_THRESHOLD_MS, "0"));
        if (leakDetectionThreshold > 0) {
            config.setLeakDetectionThreshold(leakDetectionThreshold);
        }

        String connectionTestQuery = catalogProperties.get(JdbcPoolProperties.CONNECTION_TEST_QUERY);
        if (connectionTestQuery != null && !connectionTestQuery.isBlank()) {
            config.setConnectionTestQuery(connectionTestQuery);
        }

        String user = driverProperties.get("user");
        if (user != null) {
            config.setUsername(user);
        }

        String password = driverProperties.get("password");
        if (password != null) {
            config.setPassword(password);
        }

        Properties dataSourceProperties = new Properties();
        for (Map.Entry<String, String> entry : driverProperties.entrySet()) {
            if ("user".equals(entry.getKey()) || "password".equals(entry.getKey())) {
                continue;
            }

            dataSourceProperties.put(entry.getKey(), entry.getValue());
        }
        config.setDataSourceProperties(dataSourceProperties);

        HikariDataSource dataSource = new HikariDataSource(config);
        return new JdbcConnectionPoolHandle(providerName(), dataSource, dataSource::close);
    }
}
