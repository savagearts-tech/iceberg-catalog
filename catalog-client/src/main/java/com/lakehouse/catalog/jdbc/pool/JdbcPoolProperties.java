package com.lakehouse.catalog.jdbc.pool;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.jdbc.JdbcCatalog;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class JdbcPoolProperties {

    public static final String PREFIX = "jdbc-pool.";
    public static final String PROVIDER = PREFIX + "provider";
    public static final String MAX_SIZE = PREFIX + "max-size";
    public static final String MIN_IDLE = PREFIX + "min-idle";
    public static final String CONNECTION_TIMEOUT_MS = PREFIX + "connection-timeout-ms";
    public static final String IDLE_TIMEOUT_MS = PREFIX + "idle-timeout-ms";
    public static final String MAX_LIFETIME_MS = PREFIX + "max-lifetime-ms";
    public static final String VALIDATION_TIMEOUT_MS = PREFIX + "validation-timeout-ms";
    public static final String LEAK_DETECTION_THRESHOLD_MS = PREFIX + "leak-detection-threshold-ms";
    public static final String POOL_NAME = PREFIX + "pool-name";
    public static final String CONNECTION_TEST_QUERY = PREFIX + "connection-test-query";

    private static final Set<String> ICEBERG_JDBC_PROPERTIES = Set.of(
            "schema-version",
            "init-catalog-tables",
            "strict-mode",
            "retryable-status-codes"
    );

    private JdbcPoolProperties() {
    }

    public static void putPoolProperties(
            Map<String, String> target,
            String provider,
            int maxSize,
            int minIdle,
            long connectionTimeoutMs,
            long idleTimeoutMs,
            long maxLifetimeMs,
            long validationTimeoutMs,
            long leakDetectionThresholdMs
    ) {
        target.put(PROVIDER, provider);
        target.put(MAX_SIZE, String.valueOf(maxSize));
        target.put(MIN_IDLE, String.valueOf(minIdle));
        target.put(CONNECTION_TIMEOUT_MS, String.valueOf(connectionTimeoutMs));
        target.put(IDLE_TIMEOUT_MS, String.valueOf(idleTimeoutMs));
        target.put(MAX_LIFETIME_MS, String.valueOf(maxLifetimeMs));
        target.put(VALIDATION_TIMEOUT_MS, String.valueOf(validationTimeoutMs));
        target.put(LEAK_DETECTION_THRESHOLD_MS, String.valueOf(leakDetectionThresholdMs));
        target.put(CatalogProperties.CLIENT_POOL_SIZE, String.valueOf(maxSize));
    }

    public static int configuredMaxSize(Map<String, String> properties) {
        String configured = properties.get(MAX_SIZE);
        if (configured != null) {
            return Integer.parseInt(configured);
        }

        configured = properties.get(CatalogProperties.CLIENT_POOL_SIZE);
        if (configured != null) {
            return Integer.parseInt(configured);
        }

        return 10;
    }

    public static Map<String, String> driverProperties(Map<String, String> properties) {
        Map<String, String> driverProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(JdbcCatalog.PROPERTY_PREFIX)) {
                continue;
            }

            String unprefixedKey = key.substring(JdbcCatalog.PROPERTY_PREFIX.length());
            if (ICEBERG_JDBC_PROPERTIES.contains(unprefixedKey)) {
                continue;
            }

            driverProperties.put(unprefixedKey, entry.getValue());
        }

        return driverProperties;
    }
}
