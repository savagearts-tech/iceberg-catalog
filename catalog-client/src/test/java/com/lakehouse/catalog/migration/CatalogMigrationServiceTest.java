package com.lakehouse.catalog.migration;

import com.lakehouse.catalog.config.CatalogConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CatalogMigrationServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void recordsMigratedSkippedAndFailedCounts() {
        MigrationResult result = new MigrationResult();

        result.recordNamespaceScanned();
        result.recordTableScanned();
        result.recordMigrated(TableIdentifier.of("tenant_a", "orders"));
        result.recordSkipped(TableIdentifier.of("tenant_a", "orders"));
        result.recordFailed(TableIdentifier.of("tenant_a", "payments"), new IllegalStateException("boom"));

        assertThat(result.getNamespacesScanned()).isEqualTo(1);
        assertThat(result.getTablesScanned()).isEqualTo(1);
        assertThat(result.getTablesMigrated()).isEqualTo(1);
        assertThat(result.getTablesSkipped()).isEqualTo(1);
        assertThat(result.getTablesFailed()).isEqualTo(1);
        assertThat(result.getFailures()).hasSize(1);
    }

    @Test
    void migratesTablesFromHadoopCatalogToJdbcCatalog() {
        StubHadoopCatalog sourceCatalog = new StubHadoopCatalog();
        RecordingJdbcCatalog targetCatalog = new RecordingJdbcCatalog();
        sourceCatalog.addTable(TableIdentifier.of("tenant_a", "orders"), metadataLocation("tenant_a", "orders"));

        CatalogMigrationService service = new CatalogMigrationService(sourceCatalog, targetCatalog);

        MigrationResult result = service.migrateAll();

        assertThat(result.getTablesMigrated()).isEqualTo(1);
        assertThat(targetCatalog.tableExists(TableIdentifier.of("tenant_a", "orders"))).isTrue();
    }

    @Test
    void skipsExistingTargetTables() {
        StubHadoopCatalog sourceCatalog = new StubHadoopCatalog();
        RecordingJdbcCatalog targetCatalog = new RecordingJdbcCatalog();
        TableIdentifier tableIdentifier = TableIdentifier.of("tenant_a", "orders");
        String metadataLocation = metadataLocation("tenant_a", "orders");

        sourceCatalog.addTable(tableIdentifier, metadataLocation);
        targetCatalog.registerTable(tableIdentifier, metadataLocation);

        CatalogMigrationService service = new CatalogMigrationService(sourceCatalog, targetCatalog);
        MigrationResult result = service.migrateNamespace("tenant_a");

        assertThat(result.getTablesMigrated()).isZero();
        assertThat(result.getTablesSkipped()).isEqualTo(1);
    }

    @Test
    void migratesOnlyRequestedNamespace() {
        StubHadoopCatalog sourceCatalog = new StubHadoopCatalog();
        RecordingJdbcCatalog targetCatalog = new RecordingJdbcCatalog();
        sourceCatalog.addTable(TableIdentifier.of("tenant_a", "orders"), metadataLocation("tenant_a", "orders"));
        sourceCatalog.addTable(TableIdentifier.of("tenant_b", "payments"), metadataLocation("tenant_b", "payments"));

        CatalogMigrationService service = new CatalogMigrationService(sourceCatalog, targetCatalog);
        MigrationResult result = service.migrateNamespace("tenant_a");

        assertThat(result.getNamespacesScanned()).isEqualTo(1);
        assertThat(result.getTablesMigrated()).isEqualTo(1);
        assertThat(targetCatalog.tableExists(TableIdentifier.of("tenant_a", "orders"))).isTrue();
        assertThat(targetCatalog.tableExists(TableIdentifier.of("tenant_b", "payments"))).isFalse();
    }

    @Test
    void rejectsUnsupportedCatalogTypes() {
        CatalogConfig invalidSource = CatalogConfig.builder()
                .catalogType(CatalogConfig.CatalogType.REST)
                .build();

        assertThatThrownBy(() -> new CatalogMigrationService(invalidSource, targetConfig()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source catalog");
    }

    private CatalogConfig targetConfig() {
        return CatalogConfig.builder()
                .catalogType(CatalogConfig.CatalogType.JDBC)
                .catalogName("target")
                .warehousePath(tempDir.resolve("warehouse").toUri().toString())
                .jdbcUrl("jdbc:h2:file:" + tempDir.resolve("catalog-db"))
                .jdbcUsername("sa")
                .jdbcPassword("")
                .build();
    }

    private Schema schema() {
        return new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())
        );
    }

    private String metadataLocation(String namespace, String tableName) {
        return tempDir.resolve(namespace).resolve(tableName).resolve("metadata").resolve("v1.metadata.json").toUri().toString();
    }

    private TableMetadataStub tableWithMetadata(String metadataLocation) {
        return (TableMetadataStub) Proxy.newProxyInstance(
                TableMetadataStub.class.getClassLoader(),
                new Class[]{TableMetadataStub.class},
                (proxy, method, args) -> {
                    if ("metadataFileLocation".equals(method.getName())) {
                        return metadataLocation;
                    }
                    if ("schema".equals(method.getName())) {
                        return schema();
                    }
                    if ("toString".equals(method.getName())) {
                        return "StubTable[" + metadataLocation + "]";
                    }
                    throw new UnsupportedOperationException(method.getName());
                }
        );
    }

    private interface TableMetadataStub extends org.apache.iceberg.Table, MetadataLocationSupplier {
    }

    private final class StubHadoopCatalog extends HadoopCatalog {
        private final Set<Namespace> namespaces = new LinkedHashSet<>();
        private final Map<Namespace, List<TableIdentifier>> tablesByNamespace = new HashMap<>();
        private final Map<TableIdentifier, TableMetadataStub> tables = new HashMap<>();

        void addTable(TableIdentifier tableIdentifier, String metadataLocation) {
            namespaces.add(tableIdentifier.namespace());
            tablesByNamespace.computeIfAbsent(tableIdentifier.namespace(), ignored -> new ArrayList<>()).add(tableIdentifier);
            tables.put(tableIdentifier, tableWithMetadata(metadataLocation));
        }

        @Override
        public List<Namespace> listNamespaces() {
            return new ArrayList<>(namespaces);
        }

        @Override
        public List<TableIdentifier> listTables(Namespace namespace) {
            return tablesByNamespace.getOrDefault(namespace, List.of());
        }

        @Override
        public org.apache.iceberg.Table loadTable(TableIdentifier identifier) {
            TableMetadataStub table = tables.get(identifier);
            if (table == null) {
                throw new NoSuchTableException("missing table: %s", identifier);
            }
            return table;
        }
    }

    private static final class RecordingJdbcCatalog extends JdbcCatalog {
        private final Set<Namespace> namespaces = new LinkedHashSet<>();
        private final Map<TableIdentifier, String> tables = new HashMap<>();

        @Override
        public boolean tableExists(TableIdentifier identifier) {
            return tables.containsKey(identifier);
        }

        @Override
        public org.apache.iceberg.Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
            namespaces.add(identifier.namespace());
            tables.put(identifier, metadataFileLocation);
            return null;
        }

        @Override
        public boolean namespaceExists(Namespace namespace) {
            return namespaces.contains(namespace);
        }

        @Override
        public void createNamespace(Namespace namespace, Map<String, String> metadata) {
            namespaces.add(namespace);
        }
    }
}
