# HadoopCatalog To JDBCCatalog Migration Design

## Goal

Add a reusable migration utility in `catalog-client` that migrates Iceberg table registrations from `HadoopCatalog` to `JDBCCatalog` without copying data files.

## Scope

The migration supports:

- full migration of all namespaces and tables from the source `HadoopCatalog`
- scoped migration of a single namespace
- skipping tables that already exist in the target `JDBCCatalog`
- automatic namespace creation in the target catalog when needed
- summary reporting of migrated, skipped, and failed tables

The migration does not support:

- moving or rewriting table data files
- changing table locations
- replacing existing target tables
- migrating from arbitrary source and target catalog types

## Architecture

The implementation adds a focused migration service on top of the existing `CatalogFactory` and `CatalogConfig` abstractions.

### Components

- `CatalogMigrationService`
  - builds the source and target catalogs from `CatalogConfig`
  - validates that source is `HADOOP` and target is `JDBC`
  - exposes `migrateAll()` and `migrateNamespace(String namespace)`
  - iterates namespaces and tables, loading source tables and registering them into the target catalog by metadata location

- `MigrationResult`
  - accumulates counts for scanned namespaces, scanned tables, migrated tables, skipped tables, and failed tables
  - stores failure details for later logging

- `HadoopToJdbcMigrationExample`
  - demonstrates how to configure source and target catalogs
  - shows both full migration and namespace-scoped migration entry points

## Data Flow

For each source table:

1. list namespaces from the source `HadoopCatalog`
2. list tables for each namespace
3. load the source table
4. read the current metadata file location from the loaded table operations
5. check whether the target `JDBCCatalog` already contains the same table identifier
6. if present, skip and record the skip
7. if absent, ensure the namespace exists in the target
8. register the table in `JDBCCatalog` using the source metadata location

This keeps the original table location and metadata chain intact while moving the catalog registration into JDBC-backed metadata tables.

## Error Handling

- if the source catalog type is not `HADOOP`, fail fast during service construction
- if the target catalog type is not `JDBC`, fail fast during service construction
- if one table fails to migrate, record the error and continue with the remaining tables
- if a namespace cannot be listed or created, record the failure and continue where possible

## Testing

Add focused tests in `catalog-client` that verify:

- a table can be migrated from a real `HadoopCatalog` into a real `JDBCCatalog`
- an existing target table is skipped
- namespace-scoped migration only migrates the requested namespace
- invalid source or target catalog types are rejected

The tests should use local filesystem warehouse paths and H2-backed JDBC catalog configuration so they do not depend on MinIO or REST services.
