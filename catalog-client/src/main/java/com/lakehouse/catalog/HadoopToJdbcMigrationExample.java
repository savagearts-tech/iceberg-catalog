package com.lakehouse.catalog;

import com.lakehouse.catalog.config.CatalogConfig;
import com.lakehouse.catalog.migration.CatalogMigrationService;
import com.lakehouse.catalog.migration.MigrationResult;

import java.util.HashMap;
import java.util.Map;

/**
 * Command-line entry point for migrating table registrations from HadoopCatalog to JDBCCatalog.
 */
public class HadoopToJdbcMigrationExample {

    public static void main(String[] args) {
        try {
            Command command = args.length == 0 ? new Command(sourceConfig(), targetConfig(), null) : parseArgs(args);
            CatalogMigrationService service = new CatalogMigrationService(command.sourceConfig(), command.targetConfig());
            MigrationResult result = command.namespace() == null
                    ? service.migrateAll()
                    : service.migrateNamespace(command.namespace());
            System.out.println(result);
        } catch (IllegalArgumentException exception) {
            System.err.println(exception.getMessage());
            System.err.println(usage());
        }
    }

    static CatalogConfig sourceConfig() {
        return CatalogConfig.builder()
                .catalogType(CatalogConfig.CatalogType.HADOOP)
                .catalogName("source-catalog")
                .warehousePath("s3a://lakehouse/")
                .build();
    }

    static CatalogConfig targetConfig() {
        return CatalogConfig.builder()
                .catalogType(CatalogConfig.CatalogType.JDBC)
                .catalogName("target-catalog")
                .jdbcUrl("jdbc:postgresql://localhost:5432/iceberg_catalog")
                .jdbcUsername("iceberg")
                .jdbcPassword("iceberg")
                .warehousePath("s3a://lakehouse/")
                .build();
    }

    static Command parseArgs(String[] args) {
        if (args.length == 1 && "--help".equals(args[0])) {
            throw new IllegalArgumentException("Usage requested");
        }

        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Arguments must be passed as --key value pairs.");
        }

        Map<String, String> values = new HashMap<>();
        for (int index = 0; index < args.length; index += 2) {
            values.put(args[index], args[index + 1]);
        }

        require(values,
                "--source-catalog-name",
                "--source-warehouse",
                "--target-catalog-name",
                "--target-warehouse",
                "--target-jdbc-url",
                "--target-jdbc-username",
                "--target-jdbc-password");

        CatalogConfig sourceConfig = CatalogConfig.builder()
                .catalogType(CatalogConfig.CatalogType.HADOOP)
                .catalogName(values.get("--source-catalog-name"))
                .warehousePath(values.get("--source-warehouse"))
                .minioEndpoint(values.getOrDefault("--source-minio-endpoint", "http://localhost:9000"))
                .minioAccessKey(values.getOrDefault("--source-minio-access-key", "minioadmin"))
                .minioSecretKey(values.getOrDefault("--source-minio-secret-key", "minioadmin"))
                .minioRegion(values.getOrDefault("--source-minio-region", "us-east-1"))
                .build();

        CatalogConfig targetConfig = CatalogConfig.builder()
                .catalogType(CatalogConfig.CatalogType.JDBC)
                .catalogName(values.get("--target-catalog-name"))
                .warehousePath(values.get("--target-warehouse"))
                .jdbcUrl(values.get("--target-jdbc-url"))
                .jdbcUsername(values.get("--target-jdbc-username"))
                .jdbcPassword(values.get("--target-jdbc-password"))
                .minioEndpoint(values.getOrDefault("--target-minio-endpoint", "http://localhost:9000"))
                .minioAccessKey(values.getOrDefault("--target-minio-access-key", "minioadmin"))
                .minioSecretKey(values.getOrDefault("--target-minio-secret-key", "minioadmin"))
                .minioRegion(values.getOrDefault("--target-minio-region", "us-east-1"))
                .build();

        return new Command(sourceConfig, targetConfig, values.get("--namespace"));
    }

    static String usage() {
        return """
                Usage:
                  mvn -pl catalog-client exec:java -Dexec.mainClass=com.lakehouse.catalog.HadoopToJdbcMigrationExample -Dexec.args="--source-catalog-name source --source-warehouse s3a://warehouse/ --target-catalog-name target --target-warehouse s3a://warehouse/ --target-jdbc-url jdbc:postgresql://localhost:5432/iceberg_catalog --target-jdbc-username iceberg --target-jdbc-password iceberg [--namespace tenant_a]"
                """;
    }

    private static void require(Map<String, String> values, String... requiredKeys) {
        StringBuilder missing = new StringBuilder();
        for (String requiredKey : requiredKeys) {
            if (!values.containsKey(requiredKey) || values.get(requiredKey).isBlank()) {
                if (!missing.isEmpty()) {
                    missing.append(", ");
                }
                missing.append(requiredKey);
            }
        }

        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("Missing required arguments: " + missing);
        }
    }

    record Command(CatalogConfig sourceConfig, CatalogConfig targetConfig, String namespace) {
    }
}
