# Hadoop To JDBC Catalog Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reusable Java migration utility that copies Iceberg table registrations from `HadoopCatalog` into `JDBCCatalog`, with full and namespace-scoped modes plus skip-on-existing behavior.

**Architecture:** Keep the existing `CatalogFactory` and `CatalogConfig` as the only catalog construction mechanism. Add a focused migration service that works with built catalogs, a small result object for reporting, and a runnable example entry point. Verify behavior with real `HadoopCatalog` and `JDBCCatalog` tests backed by temporary local storage and H2.

**Tech Stack:** Java 17, Maven, Apache Iceberg 1.7.1, HadoopCatalog, JDBCCatalog, JUnit 5, AssertJ, H2

---

### Task 1: Add migration result model

**Files:**
- Create: `catalog-client/src/main/java/com/lakehouse/catalog/migration/MigrationResult.java`

- [ ] **Step 1: Write the failing test**

```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl catalog-client -Dtest=CatalogMigrationServiceTest#recordsMigratedSkippedAndFailedCounts test`

Expected: FAIL because `MigrationResult` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```java
package com.lakehouse.catalog.migration;

import lombok.Getter;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.ArrayList;
import java.util.List;

@Getter
public class MigrationResult {
    private int namespacesScanned;
    private int tablesScanned;
    private int tablesMigrated;
    private int tablesSkipped;
    private int tablesFailed;
    private final List<String> failures = new ArrayList<>();

    public void recordNamespaceScanned() {
        namespacesScanned++;
    }

    public void recordTableScanned() {
        tablesScanned++;
    }

    public void recordMigrated(TableIdentifier tableIdentifier) {
        tablesMigrated++;
    }

    public void recordSkipped(TableIdentifier tableIdentifier) {
        tablesSkipped++;
    }

    public void recordFailed(TableIdentifier tableIdentifier, Exception exception) {
        tablesFailed++;
        failures.add(tableIdentifier + ": " + exception.getMessage());
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl catalog-client -Dtest=CatalogMigrationServiceTest#recordsMigratedSkippedAndFailedCounts test`

Expected: PASS

### Task 2: Add migration service

**Files:**
- Create: `catalog-client/src/main/java/com/lakehouse/catalog/migration/CatalogMigrationService.java`
- Modify: `catalog-client/src/main/java/com/lakehouse/catalog/factory/CatalogFactory.java`
- Test: `catalog-client/src/test/java/com/lakehouse/catalog/migration/CatalogMigrationServiceTest.java`

- [ ] **Step 1: Write the failing tests**

```java
@Test
void migratesTablesFromHadoopCatalogToJdbcCatalog() {
    CatalogMigrationService service = new CatalogMigrationService(sourceConfig, targetConfig);

    MigrationResult result = service.migrateAll();

    assertThat(result.getTablesMigrated()).isEqualTo(1);
    assertThat(targetCatalog.tableExists(TableIdentifier.of("tenant_a", "orders"))).isTrue();
}

@Test
void skipsExistingTargetTables() {
    targetCatalog.registerTable(tableId, metadataLocation);

    CatalogMigrationService service = new CatalogMigrationService(sourceConfig, targetConfig);
    MigrationResult result = service.migrateNamespace("tenant_a");

    assertThat(result.getTablesMigrated()).isZero();
    assertThat(result.getTablesSkipped()).isEqualTo(1);
}

@Test
void rejectsUnsupportedCatalogTypes() {
    CatalogConfig invalidSource = CatalogConfig.builder().catalogType(CatalogConfig.CatalogType.REST).build();

    assertThatThrownBy(() -> new CatalogMigrationService(invalidSource, targetConfig))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("source catalog");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -pl catalog-client -Dtest=CatalogMigrationServiceTest test`

Expected: FAIL because the service does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```java
package com.lakehouse.catalog.migration;

import com.lakehouse.catalog.config.CatalogConfig;
import com.lakehouse.catalog.factory.CatalogFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;

import java.util.List;

@Slf4j
public class CatalogMigrationService {
    private final HadoopCatalog sourceCatalog;
    private final JdbcCatalog targetCatalog;

    public CatalogMigrationService(CatalogConfig sourceConfig, CatalogConfig targetConfig) {
        this.sourceCatalog = asHadoopCatalog(CatalogFactory.build(sourceConfig));
        this.targetCatalog = asJdbcCatalog(CatalogFactory.build(targetConfig));
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
        result.recordNamespaceScanned();
        ensureNamespace(namespace);

        for (TableIdentifier tableIdentifier : sourceCatalog.listTables(namespace)) {
            result.recordTableScanned();
            if (targetCatalog.tableExists(tableIdentifier)) {
                result.recordSkipped(tableIdentifier);
                continue;
            }

            try {
                Table table = sourceCatalog.loadTable(tableIdentifier);
                String metadataLocation = table.operations().current().metadataFileLocation();
                targetCatalog.registerTable(tableIdentifier, metadataLocation);
                result.recordMigrated(tableIdentifier);
            } catch (Exception exception) {
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -pl catalog-client -Dtest=CatalogMigrationServiceTest test`

Expected: PASS

### Task 3: Add runnable example entry point

**Files:**
- Create: `catalog-client/src/main/java/com/lakehouse/catalog/HadoopToJdbcMigrationExample.java`

- [ ] **Step 1: Write the failing smoke test**

```java
@Test
void exampleCanConstructMigrationService() {
    assertThatCode(HadoopToJdbcMigrationExample::sourceConfig).doesNotThrowAnyException();
    assertThatCode(HadoopToJdbcMigrationExample::targetConfig).doesNotThrowAnyException();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl catalog-client -Dtest=CatalogMigrationExampleTest test`

Expected: FAIL because the example does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```java
package com.lakehouse.catalog;

import com.lakehouse.catalog.config.CatalogConfig;
import com.lakehouse.catalog.migration.CatalogMigrationService;
import com.lakehouse.catalog.migration.MigrationResult;

public class HadoopToJdbcMigrationExample {

    public static void main(String[] args) {
        CatalogMigrationService service = new CatalogMigrationService(sourceConfig(), targetConfig());
        MigrationResult result = args.length > 0 ? service.migrateNamespace(args[0]) : service.migrateAll();
        System.out.println(result);
    }

    static CatalogConfig sourceConfig() {
        return CatalogConfig.builder()
                .catalogType(CatalogConfig.CatalogType.HADOOP)
                .warehousePath("s3a://lakehouse/")
                .build();
    }

    static CatalogConfig targetConfig() {
        return CatalogConfig.builder()
                .catalogType(CatalogConfig.CatalogType.JDBC)
                .jdbcUrl("jdbc:postgresql://localhost:5432/iceberg_catalog")
                .jdbcUsername("iceberg")
                .jdbcPassword("iceberg")
                .warehousePath("s3a://lakehouse/")
                .build();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl catalog-client -Dtest=CatalogMigrationExampleTest test`

Expected: PASS

### Task 4: Verify the final behavior

**Files:**
- Test: `catalog-client/src/test/java/com/lakehouse/catalog/migration/CatalogMigrationServiceTest.java`

- [ ] **Step 1: Run focused migration tests**

Run: `mvn -pl catalog-client -Dtest=CatalogMigrationServiceTest,CatalogMigrationExampleTest test`

Expected: PASS

- [ ] **Step 2: Run the module test suite**

Run: `mvn -pl catalog-client test`

Expected: PASS
