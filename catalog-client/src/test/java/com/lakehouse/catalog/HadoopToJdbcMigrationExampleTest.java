package com.lakehouse.catalog;

import com.lakehouse.catalog.config.CatalogConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HadoopToJdbcMigrationExampleTest {

    @Test
    void exampleCanConstructConfigs() {
        assertThatCode(HadoopToJdbcMigrationExample::sourceConfig).doesNotThrowAnyException();
        assertThatCode(HadoopToJdbcMigrationExample::targetConfig).doesNotThrowAnyException();
    }

    @Test
    void parsesRequiredCommandArguments() {
        HadoopToJdbcMigrationExample.Command command = HadoopToJdbcMigrationExample.parseArgs(new String[]{
                "--source-catalog-name", "source",
                "--source-warehouse", "s3a://warehouse/",
                "--target-catalog-name", "target",
                "--target-warehouse", "s3a://warehouse/",
                "--target-jdbc-url", "jdbc:h2:mem:test",
                "--target-jdbc-username", "sa",
                "--target-jdbc-password", "secret"
        });

        CatalogConfig sourceConfig = command.sourceConfig();
        CatalogConfig targetConfig = command.targetConfig();

        assertThat(sourceConfig.getCatalogType()).isEqualTo(CatalogConfig.CatalogType.HADOOP);
        assertThat(sourceConfig.getCatalogName()).isEqualTo("source");
        assertThat(sourceConfig.getWarehousePath()).isEqualTo("s3a://warehouse/");
        assertThat(targetConfig.getCatalogType()).isEqualTo(CatalogConfig.CatalogType.JDBC);
        assertThat(targetConfig.getCatalogName()).isEqualTo("target");
        assertThat(targetConfig.getJdbcUrl()).isEqualTo("jdbc:h2:mem:test");
        assertThat(targetConfig.getJdbcUsername()).isEqualTo("sa");
        assertThat(targetConfig.getJdbcPassword()).isEqualTo("secret");
        assertThat(command.namespace()).isNull();
    }

    @Test
    void parsesOptionalNamespaceArgument() {
        HadoopToJdbcMigrationExample.Command command = HadoopToJdbcMigrationExample.parseArgs(new String[]{
                "--source-catalog-name", "source",
                "--source-warehouse", "s3a://warehouse/",
                "--target-catalog-name", "target",
                "--target-warehouse", "s3a://warehouse/",
                "--target-jdbc-url", "jdbc:h2:mem:test",
                "--target-jdbc-username", "sa",
                "--target-jdbc-password", "secret",
                "--namespace", "tenant_a"
        });

        assertThat(command.namespace()).isEqualTo("tenant_a");
    }

    @Test
    void rejectsMissingRequiredArguments() {
        assertThatThrownBy(() -> HadoopToJdbcMigrationExample.parseArgs(new String[]{
                "--source-catalog-name", "source"
        }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--source-warehouse")
                .hasMessageContaining("--target-jdbc-url");
    }
}
