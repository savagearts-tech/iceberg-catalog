package com.lakehouse.catalog.jdbc.pool;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.JdbcClientPool;

import java.io.Closeable;
import java.util.Map;

/**
 * JDBC catalog with a pluggable connection pool. Implements {@link Closeable} so callers
 * using try-with-resources or {@code instanceof Closeable} (for example {@code IcebergCatalogClient})
 * reliably invoke {@link JdbcCatalog#close()} and shut down the pool.
 */
public class PooledJdbcCatalog extends JdbcCatalog implements Closeable {

    public PooledJdbcCatalog() {
        super(null, PooledJdbcCatalog::buildClientPool, true);
    }

    private static JdbcClientPool buildClientPool(Map<String, String> properties) {
        String jdbcUrl = properties.get(CatalogProperties.URI);
        JdbcConnectionPoolProvider provider = JdbcConnectionPoolProviderFactory.load(properties);
        JdbcConnectionPoolHandle poolHandle = provider.create(
                jdbcUrl,
                properties,
                JdbcPoolProperties.driverProperties(properties)
        );
        return new PooledJdbcClientPool(jdbcUrl, properties, poolHandle);
    }
}
