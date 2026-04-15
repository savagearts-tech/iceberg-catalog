package com.lakehouse.catalog.migration;

import lombok.Getter;
import lombok.ToString;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.ArrayList;
import java.util.List;

/**
 * Summary of a catalog migration run.
 */
@Getter
@ToString
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
