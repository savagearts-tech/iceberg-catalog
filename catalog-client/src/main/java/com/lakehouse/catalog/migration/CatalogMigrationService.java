package com.lakehouse.catalog.migration;

import com.lakehouse.catalog.config.CatalogConfig;
import com.lakehouse.catalog.factory.CatalogFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;

/**
 * Migrates Iceberg table registrations from HadoopCatalog to JDBCCatalog.
 */
@Slf4j
public class CatalogMigrationService {

    private final HadoopCatalog sourceCatalog;
    private final JdbcCatalog targetCatalog;

    public CatalogMigrationService(CatalogConfig sourceConfig, CatalogConfig targetConfig) {
        validateConfigTypes(sourceConfig, targetConfig);
        this.sourceCatalog = asHadoopCatalog(CatalogFactory.build(sourceConfig));
        this.targetCatalog = asJdbcCatalog(CatalogFactory.build(targetConfig));
    }

    CatalogMigrationService(HadoopCatalog sourceCatalog, JdbcCatalog targetCatalog) {
        this.sourceCatalog = sourceCatalog;
        this.targetCatalog = targetCatalog;
    }

    public MigrationResult migrateAll() {
        MigrationResult result = new MigrationResult();
        for (Namespace namespace : sourceCatalog.listNamespaces()) {
            migrateNamespace(namespace, result);
        }
        return result;
    }

    public MigrationResult migrateNamespace(String namespace) {
        MigrationResult result = new MigrationResult();
        migrateNamespace(Namespace.of(namespace), result);
        return result;
    }

    private void migrateNamespace(Namespace namespace, MigrationResult result) {
        log.info("Migrating namespace: {}", namespace);
        result.recordNamespaceScanned();
        ensureNamespace(namespace);

        for (TableIdentifier tableIdentifier : sourceCatalog.listTables(namespace)) {
            result.recordTableScanned();

            if (targetCatalog.tableExists(tableIdentifier)) {
                log.info("Skipping existing table: {}", tableIdentifier);
                result.recordSkipped(tableIdentifier);
                continue;
            }

            try {
                Table sourceTable = sourceCatalog.loadTable(tableIdentifier);
                String metadataLocation = metadataLocation(sourceTable);
                targetCatalog.registerTable(tableIdentifier, metadataLocation);
                log.info("Migrated table: {} -> {}", tableIdentifier, metadataLocation);
                result.recordMigrated(tableIdentifier);
            } catch (Exception exception) {
                log.error("Failed to migrate table {}", tableIdentifier, exception);
                result.recordFailed(tableIdentifier, exception);
            }
        }
    }

    private void ensureNamespace(Namespace namespace) {
        SupportsNamespaces namespaces = targetCatalog;
        if (!namespaces.namespaceExists(namespace)) {
            namespaces.createNamespace(namespace);
        }
    }

    private static String metadataLocation(Table table) {
        if (table instanceof MetadataLocationSupplier supplier) {
            return supplier.metadataFileLocation();
        }
        if (table instanceof BaseTable baseTable) {
            return baseTable.operations().current().metadataFileLocation();
        }
        throw new IllegalArgumentException("unsupported table implementation: " + table.getClass().getName());
    }

    private static void validateConfigTypes(CatalogConfig sourceConfig, CatalogConfig targetConfig) {
        if (sourceConfig.getCatalogType() != CatalogConfig.CatalogType.HADOOP) {
            throw new IllegalArgumentException("source catalog must be HADOOP");
        }
        if (targetConfig.getCatalogType() != CatalogConfig.CatalogType.JDBC) {
            throw new IllegalArgumentException("target catalog must be JDBC");
        }
    }

    private static HadoopCatalog asHadoopCatalog(Catalog catalog) {
        if (catalog instanceof HadoopCatalog hadoopCatalog) {
            return hadoopCatalog;
        }
        throw new IllegalArgumentException("source catalog must be HADOOP");
    }

    private static JdbcCatalog asJdbcCatalog(Catalog catalog) {
        if (catalog instanceof JdbcCatalog jdbcCatalog) {
            return jdbcCatalog;
        }
        throw new IllegalArgumentException("target catalog must be JDBC");
    }
}
