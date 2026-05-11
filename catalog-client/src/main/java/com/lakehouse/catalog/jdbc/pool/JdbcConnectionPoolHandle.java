package com.lakehouse.catalog.jdbc.pool;

import javax.sql.DataSource;

public record JdbcConnectionPoolHandle(String providerName, DataSource dataSource, Runnable closeAction)
        implements AutoCloseable {

    @Override
    public void close() {
        closeAction.run();
    }
}
