package com.lakehouse.catalog.client;

import com.lakehouse.catalog.config.CatalogConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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
