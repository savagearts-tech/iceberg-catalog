package com.lakehouse.catalog.factory;

import com.lakehouse.catalog.config.CatalogConfig;
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
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, config.getWarehousePath());
        properties.put(CatalogProperties.URI, config.getJdbcUrl());
        properties.put("jdbc.user", config.getJdbcUsername());
        properties.put("jdbc.password", config.getJdbcPassword());
        applyWarehouseFileIo(properties, config);

        JdbcCatalog jdbcCatalog = new JdbcCatalog();
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

    private static boolean isS3Warehouse(String warehousePath) {
        return warehousePath != null
                && (warehousePath.startsWith("s3://") || warehousePath.startsWith("s3a://"));
    }
}
