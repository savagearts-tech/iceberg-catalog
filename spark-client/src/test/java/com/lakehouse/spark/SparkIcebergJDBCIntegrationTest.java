package com.lakehouse.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: Spark SQL ↔ Iceberg JDBC Catalog ↔ local warehouse.
 *
 * <p>This test uses an embedded H2 catalog database plus local filesystem
 * warehouse storage so it can verify JDBC catalog queries without depending
 * on REST catalog services or object storage.</p>
 */
class SparkIcebergJDBCIntegrationTest {

    private static final Path FAKE_HADOOP_HOME = prepareFakeHadoopHome();

    @TempDir
    Path tempDir;

    private SparkSession spark;
    private String catalogName;

    @BeforeEach
    void setUp() {
        assertThat(FAKE_HADOOP_HOME).exists();

        catalogName = "jdbc_lakehouse_" + UUID.randomUUID().toString().replace("-", "");
        String warehousePath = tempDir.resolve("warehouse").toUri().toString();
        String jdbcUri = "jdbc:h2:file:" + normalizePath(tempDir.resolve("catalog-db"));

        spark = SparkSession.builder()
                .appName("Spark JDBC Catalog Integration Test")
                .master("local[*]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog." + catalogName, "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog." + catalogName + ".type", "jdbc")
                .config("spark.sql.catalog." + catalogName + ".uri", jdbcUri)
                .config("spark.sql.catalog." + catalogName + ".warehouse", warehousePath)
                .config("spark.sql.catalog." + catalogName + ".io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
                .config("spark.sql.catalog." + catalogName + ".jdbc.schema-version", "V1")
                .config("spark.sql.defaultCatalog", catalogName)
                .getOrCreate();

        spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".db");
    }

    @AfterEach
    void tearDown() {
        if (spark != null) {
            List<Row> tables = spark.sql("SHOW TABLES IN " + catalogName + ".db").collectAsList();
            for (Row table : tables) {
                spark.sql("DROP TABLE IF EXISTS " + catalogName + ".db." + table.getString(1) + " PURGE");
            }
            spark.stop();
        }
    }

    @Test
    @DisplayName("should_QueryAggregatedData_When_UsingJdbcCatalog")
    void should_QueryAggregatedData_When_UsingJdbcCatalog() {
        spark.sql("CREATE TABLE " + catalogName + ".db.orders (" +
                "  order_id BIGINT, " +
                "  user_id BIGINT, " +
                "  amount DOUBLE, " +
                "  dt STRING" +
                ") USING iceberg");

        spark.sql("INSERT INTO " + catalogName + ".db.orders VALUES " +
                "(1, 1001, 99.90, '2026-04-15'), " +
                "(2, 1001, 59.00, '2026-04-15'), " +
                "(3, 1002, 199.00, '2026-04-15'), " +
                "(4, 1003, 39.90, '2026-04-16')");

        Dataset<Row> result = spark.sql(
                "SELECT user_id, COUNT(*) AS order_cnt, ROUND(SUM(amount), 2) AS total_amount " +
                        "FROM " + catalogName + ".db.orders " +
                        "WHERE dt = '2026-04-15' " +
                        "GROUP BY user_id " +
                        "ORDER BY total_amount DESC");

        List<Row> rows = result.collectAsList();
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getLong(0)).isEqualTo(1002L);
        assertThat(rows.get(0).getLong(1)).isEqualTo(1L);
        assertThat(rows.get(0).getDouble(2)).isEqualTo(199.00);
        assertThat(rows.get(1).getLong(0)).isEqualTo(1001L);
        assertThat(rows.get(1).getLong(1)).isEqualTo(2L);
        assertThat(rows.get(1).getDouble(2)).isEqualTo(158.90);
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
