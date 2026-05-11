package com.lakehouse.catalog.jdbc.pool;

import java.lang.reflect.InvocationTargetException;
import java.util.Locale;
import java.util.Map;

public final class JdbcConnectionPoolProviderFactory {

    private JdbcConnectionPoolProviderFactory() {
    }

    public static JdbcConnectionPoolProvider load(Map<String, String> properties) {
        String configuredProvider = properties.getOrDefault(JdbcPoolProperties.PROVIDER, "hikari");
        if ("hikari".equalsIgnoreCase(configuredProvider)) {
            return new HikariJdbcConnectionPoolProvider();
        }

        try {
            Class<?> providerClass = Class.forName(configuredProvider);
            Object instance = providerClass.getDeclaredConstructor().newInstance();
            if (instance instanceof JdbcConnectionPoolProvider provider) {
                return provider;
            }

            throw new IllegalArgumentException(
                    "Configured JDBC pool provider does not implement JdbcConnectionPoolProvider: "
                            + configuredProvider);
        } catch (ClassNotFoundException
                 | InstantiationException
                 | IllegalAccessException
                 | InvocationTargetException
                 | NoSuchMethodException exception) {
            throw new IllegalArgumentException(
                    "Unknown JDBC pool provider: " + configuredProvider.toLowerCase(Locale.ROOT),
                    exception);
        }
    }
}
