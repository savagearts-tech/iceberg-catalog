package com.lakehouse.catalog.factory;

import com.lakehouse.catalog.config.CatalogConfig;
import com.lakehouse.catalog.config.CatalogConfig.CatalogType;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CatalogFactoryTest {

    @Test
    void testBuildRestCatalog() {
        CatalogConfig config = CatalogConfig.builder()
                .catalogType(CatalogType.REST)
                .catalogUri("http://localhost:9001/iceberg/v1")
                .build();

        assertThatThrownBy(() -> CatalogFactory.build(config))
                .isInstanceOf(RESTException.class)
                .hasMessageContaining("GET request");
    }

    @Test
    void testBuildJdbcCatalog() {
        CatalogConfig config = CatalogConfig.builder()
                .catalogType(CatalogType.JDBC)
                .jdbcUrl("jdbc:h2:mem:test_db;DB_CLOSE_DELAY=-1")
                .build();
        
        Catalog catalog = CatalogFactory.build(config);
        assertThat(catalog).isInstanceOf(JdbcCatalog.class);
        assertThat(catalog.name()).isEqualTo("lakehouse");
    }

    @Test
    void testBuildHadoopCatalog() {
        CatalogConfig config = CatalogConfig.builder()
                .catalogType(CatalogType.HADOOP)
                .warehousePath("file:///tmp/warehouse")
                .build();
        
        Catalog catalog = CatalogFactory.build(config);
        assertThat(catalog).isInstanceOf(HadoopCatalog.class);
        assertThat(catalog.name()).isEqualTo("lakehouse");
    }
}
