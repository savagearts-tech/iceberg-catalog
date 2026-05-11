package com.lakehouse.catalog.client;

import com.lakehouse.catalog.config.CatalogConfig;
import com.lakehouse.catalog.jdbc.pool.JdbcConnectionPoolHandle;
import com.lakehouse.catalog.jdbc.pool.PooledJdbcClientPool;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergCatalogClientJdbcCatalogTest {

    private static final String NAMESPACE = "jdbc_tenant";
    private static final Path FAKE_HADOOP_HOME = prepareFakeHadoopHome();
    private static final Schema USER_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get())
    );

    @TempDir
    Path tempDir;

    private IcebergCatalogClient client;

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    @DisplayName("should_WriteParquetFiles_When_UsingJdbcCatalog")
    void should_WriteParquetFiles_When_UsingJdbcCatalog() throws IOException {
        Path warehouseDir = tempDir.resolve("warehouse");
        CatalogConfig config = CatalogConfig.builder()
                .catalogType(CatalogConfig.CatalogType.JDBC)
                .catalogName("jdbc-test-catalog")
                .defaultNamespace(NAMESPACE)
                .warehousePath(warehouseDir.toUri().toString())
                .jdbcUrl("jdbc:h2:file:" + normalizePath(tempDir.resolve("iceberg-catalog-db")))
                .jdbcUsername("sa")
                .jdbcPassword("")
                .jdbcPoolProvider("hikari")
                .jdbcPoolMaxSize(4)
                .jdbcPoolMinIdle(1)
                .build();

        client = new IcebergCatalogClient(config);
        assertThat(FAKE_HADOOP_HOME).exists();
        client.ensureNamespace();

        Table table = client.createTable("users", USER_SCHEMA);
        client.writeRecords("users", createSampleRecords());
        Table updatedTable = client.loadTable("users");

        assertThat(table).isNotNull();
        assertThat(updatedTable.currentSnapshot()).isNotNull();

        try (var paths = Files.walk(warehouseDir)) {
            List<Path> parquetFiles = paths
                    .filter(path -> path.getFileName() != null)
                    .filter(path -> path.getFileName().toString().endsWith(".parquet"))
                    .toList();

            assertThat(parquetFiles).isNotEmpty();
        }
    }

    @Test
    @DisplayName("should_CloseHikariPool_When_ClientClosed")
    void should_CloseHikariPool_When_ClientClosed() throws Exception {
        Path warehouseDir = tempDir.resolve("warehouse-close-test");
        CatalogConfig config = CatalogConfig.builder()
                .catalogType(CatalogConfig.CatalogType.JDBC)
                .catalogName("jdbc-close-test-catalog")
                .defaultNamespace(NAMESPACE)
                .warehousePath(warehouseDir.toUri().toString())
                .jdbcUrl("jdbc:h2:file:" + normalizePath(tempDir.resolve("iceberg-catalog-db-close")))
                .jdbcUsername("sa")
                .jdbcPassword("")
                .jdbcPoolProvider("hikari")
                .jdbcPoolMaxSize(2)
                .jdbcPoolMinIdle(1)
                .build();

        client = new IcebergCatalogClient(config);
        Catalog catalog = readCatalogField(client);
        assertThat(catalog).isInstanceOf(Closeable.class);
        client.ensureNamespace();

        HikariDataSource dataSource = hikariDataSourceFromJdbcCatalog((JdbcCatalog) catalog);
        client.close();
        client = null;

        assertThat(dataSource.isClosed()).isTrue();
    }

    @Test
    @DisplayName("should_CreatePartitionedTable_When_UsingJdbcCatalogWithBucketSpec")
    void should_CreatePartitionedTable_When_UsingJdbcCatalogWithBucketSpec() {
        Path warehouseDir = tempDir.resolve("warehouse-partitioned");
        CatalogConfig config = CatalogConfig.builder()
                .catalogType(CatalogConfig.CatalogType.JDBC)
                .catalogName("jdbc-partitioned-catalog")
                .defaultNamespace(NAMESPACE)
                .warehousePath(warehouseDir.toUri().toString())
                .jdbcUrl("jdbc:h2:file:" + normalizePath(tempDir.resolve("iceberg-catalog-db-part")))
                .jdbcUsername("sa")
                .jdbcPassword("")
                .jdbcPoolProvider("hikari")
                .jdbcPoolMaxSize(2)
                .jdbcPoolMinIdle(1)
                .build();

        client = new IcebergCatalogClient(config);
        client.ensureNamespace();

        PartitionSpec spec = PartitionSpec.builderFor(USER_SCHEMA).bucket("id", 4).build();
        Table table = client.createTable(
                "partitioned_users",
                USER_SCHEMA,
                spec,
                Map.of("comment", "partitioned jdbc test"));

        assertThat(table.spec().isPartitioned()).isTrue();
        assertThat(table.properties()).containsEntry("comment", "partitioned jdbc test");
    }

    private static Catalog readCatalogField(IcebergCatalogClient icebergClient) throws Exception {
        Field field = IcebergCatalogClient.class.getDeclaredField("catalog");
        field.setAccessible(true);
        return (Catalog) field.get(icebergClient);
    }

    private static HikariDataSource hikariDataSourceFromJdbcCatalog(JdbcCatalog jdbcCatalog) throws Exception {
        Method connectionPool = JdbcCatalog.class.getDeclaredMethod("connectionPool");
        connectionPool.setAccessible(true);
        Object pool = connectionPool.invoke(jdbcCatalog);
        assertThat(pool).isInstanceOf(PooledJdbcClientPool.class);

        Field poolHandleField = PooledJdbcClientPool.class.getDeclaredField("poolHandle");
        poolHandleField.setAccessible(true);
        JdbcConnectionPoolHandle handle = (JdbcConnectionPoolHandle) poolHandleField.get(pool);
        return (HikariDataSource) handle.dataSource();
    }

    private List<GenericRecord> createSampleRecords() {
        GenericRecord alice = GenericRecord.create(USER_SCHEMA);
        alice.setField("id", 1L);
        alice.setField("name", "alice");
        alice.setField("email", "alice@test.com");

        GenericRecord bob = GenericRecord.create(USER_SCHEMA);
        bob.setField("id", 2L);
        bob.setField("name", "bob");
        bob.setField("email", "bob@test.com");

        return List.of(alice, bob);
    }

    private String normalizePath(Path path) {
        return path.toAbsolutePath().toString().replace("\\", "/");
    }

    private static Path prepareFakeHadoopHome() {
        try {
            Path hadoopHome = Files.createTempDirectory("fake-hadoop");
            Path binDir = Files.createDirectories(hadoopHome.resolve("bin"));
            Path winutilsExe = binDir.resolve("winutils.exe");
            String powershellScript = """
                    $source = @'
                    using System;
                    public static class Program
                    {
                        public static int Main(string[] args)
                        {
                            return 0;
                        }
                    }
                    '@;
                    Add-Type -TypeDefinition $source -OutputAssembly '%s' -OutputType ConsoleApplication -Language CSharp
                    """.formatted(winutilsExe);

            Process process = new ProcessBuilder(
                    "powershell.exe",
                    "-NoProfile",
                    "-Command",
                    powershellScript
            ).start();

            String errorOutput = new String(process.getErrorStream().readAllBytes());
            int exitCode = process.waitFor();
            if (exitCode != 0 || !Files.exists(winutilsExe)) {
                throw new IllegalStateException("failed to create fake winutils.exe: " + errorOutput);
            }

            System.setProperty("hadoop.home.dir", hadoopHome.toString());
            return hadoopHome;
        } catch (IOException | InterruptedException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }
}
