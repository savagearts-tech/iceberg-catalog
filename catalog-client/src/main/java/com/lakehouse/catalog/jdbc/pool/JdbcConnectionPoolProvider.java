package com.lakehouse.catalog.jdbc.pool;

import java.util.Map;

public interface JdbcConnectionPoolProvider {

    String providerName();

    JdbcConnectionPoolHandle create(
            String jdbcUrl,
            Map<String, String> catalogProperties,
            Map<String, String> driverProperties
    );
}
