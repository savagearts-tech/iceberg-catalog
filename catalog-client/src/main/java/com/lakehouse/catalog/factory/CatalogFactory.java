package com.lakehouse.catalog.factory;

import com.lakehouse.catalog.config.CatalogConfig;
import com.lakehouse.catalog.jdbc.pool.JdbcPoolProperties;
import com.lakehouse.catalog.jdbc.pool.PooledJdbcCatalog;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.RESTCatalog;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory for creating Iceberg Catalog instances based on configuration.
 *
 * @author platform
 * @since 1.0.0
 */
@Slf4j
public class CatalogFactory {

    /**
     * Builds and initializes an Iceberg Catalog based on the provided configuration.
     *
     * @param config the catalog configuration
     * @return the initialized catalog instance
     */
    public static Catalog build(CatalogConfig config) {
        log.info("Building Iceberg Catalog of type: {}", config.getCatalogType());
        Configuration hadoopConf = buildHadoopConf(config);

        return switch (config.getCatalogType()) {
            case HADOOP -> buildHadoopCatalog(config, hadoopConf);
            case JDBC -> buildJdbcCatalog(config, hadoopConf);
            case REST -> buildRESTCatalog(config, hadoopConf);
        };
    }

    private static HadoopCatalog buildHadoopCatalog(CatalogConfig config, Configuration hadoopConf) {
        HadoopCatalog hadoopCatalog = new HadoopCatalog();
        hadoopCatalog.setConf(hadoopConf);
        hadoopCatalog.initialize(config.getCatalogName(), Map.of(
                CatalogProperties.WAREHOUSE_LOCATION, config.getWarehousePath()
        ));
        return hadoopCatalog;
    }

    private static JdbcCatalog buildJdbcCatalog(CatalogConfig config, Configuration hadoopConf) {
        Map<String, String> properties = new HashMap<>();
        properties.putAll(config.getCatalogProperties());
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, config.getWarehousePath());
        properties.put(CatalogProperties.URI, config.getJdbcUrl());
        properties.put("jdbc.user", config.getJdbcUsername());
        properties.put("jdbc.password", config.getJdbcPassword());
        applyJdbcDriverProperties(properties, config);
        applyJdbcPoolProperties(properties, config);
        applyWarehouseFileIo(properties, config);

        JdbcCatalog jdbcCatalog = new PooledJdbcCatalog();
        jdbcCatalog.setConf(hadoopConf);
        jdbcCatalog.initialize(config.getCatalogName(), properties);
        return jdbcCatalog;
    }

    private static RESTCatalog buildRESTCatalog(CatalogConfig config, Configuration hadoopConf) {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
        properties.put(CatalogProperties.URI, config.getCatalogUri());
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, config.getWarehousePath());
        applyWarehouseFileIo(properties, config);

        RESTCatalog restCatalog = new RESTCatalog();
        restCatalog.setConf(hadoopConf);
        restCatalog.initialize(config.getCatalogName(), properties);
        return restCatalog;
    }

    private static Configuration buildHadoopConf(CatalogConfig config) {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.endpoint", config.getMinioEndpoint());
        hadoopConf.set("fs.s3a.access.key", config.getMinioAccessKey());
        hadoopConf.set("fs.s3a.secret.key", config.getMinioSecretKey());
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.region", config.getMinioRegion());
        return hadoopConf;
    }

    private static void applyWarehouseFileIo(Map<String, String> properties, CatalogConfig config) {
        if (isS3Warehouse(config.getWarehousePath())) {
            properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
            properties.put("s3.endpoint", config.getMinioEndpoint());
            properties.put("s3.access-key-id", config.getMinioAccessKey());
            properties.put("s3.secret-access-key", config.getMinioSecretKey());
            properties.put("s3.region", config.getMinioRegion());
            properties.put("s3.path-style-access", "true");
            properties.put("client.region", config.getMinioRegion());
            return;
        }

        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");
    }

    private static void applyJdbcDriverProperties(Map<String, String> properties, CatalogConfig config) {
        for (Map.Entry<String, String> entry : config.getJdbcProperties().entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(JdbcCatalog.PROPERTY_PREFIX)) {
                properties.put(key, entry.getValue());
            } else {
                properties.put(JdbcCatalog.PROPERTY_PREFIX + key, entry.getValue());
            }
        }
    }

    private static void applyJdbcPoolProperties(Map<String, String> properties, CatalogConfig config) {
        JdbcPoolProperties.putPoolProperties(
                properties,
                config.getJdbcPoolProvider(),
                config.getJdbcPoolMaxSize(),
                config.getJdbcPoolMinIdle(),
                config.getJdbcPoolConnectionTimeoutMs(),
                config.getJdbcPoolIdleTimeoutMs(),
                config.getJdbcPoolMaxLifetimeMs(),
                config.getJdbcPoolValidationTimeoutMs(),
                config.getJdbcPoolLeakDetectionThresholdMs()
        );
    }

    private static boolean isS3Warehouse(String warehousePath) {
        return warehousePath != null
                && (warehousePath.startsWith("s3://") || warehousePath.startsWith("s3a://"));
    }
}
