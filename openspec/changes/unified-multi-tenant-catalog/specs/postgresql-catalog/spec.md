## ADDED Requirements

### Requirement: PostgreSQL JDBC Catalog registration
The system SHALL register PostgreSQL as a JDBC Catalog in Gravitino using `provider=jdbc-postgresql`, enabling unified metadata discovery of PostgreSQL schemas, tables, views, and column types.

#### Scenario: PostgreSQL catalog registration
- **WHEN** a platform administrator registers a PostgreSQL catalog with valid JDBC URI (`jdbc:postgresql://host:5432/db`)
- **THEN** Gravitino SHALL list all non-system schemas (public, etc.) via `GET /api/metalakes/.../catalogs/{catalog}/schemas`

#### Scenario: Column type mapping validation
- **WHEN** Gravitino discovers a PostgreSQL table with columns of types `uuid`, `jsonb`, `timestamptz`
- **THEN** the returned column metadata SHALL map these to Gravitino's type system (`STRING`, `STRING`, `TIMESTAMP_WITH_TIME_ZONE` respectively)

### Requirement: PostgreSQL schema filtering
The system SHALL support configuring a schema allowlist to restrict which PostgreSQL schemas are visible through Gravitino, preventing exposure of system or sensitive schemas.

#### Scenario: Schema allowlist enforcement
- **WHEN** a catalog is configured with `schema.allowlist=public,analytics`
- **THEN** only `public` and `analytics` schemas SHALL appear in catalog listing; system schemas (pg_catalog, information_schema) SHALL be hidden

### Requirement: PostgreSQL catalog supports DDL for authorized users
Unlike ClickHouse, the PostgreSQL JDBC Catalog SHALL allow DDL operations (CREATE TABLE, DROP TABLE) for authorized platform administrators with appropriate Gravitino permissions.

#### Scenario: Table creation through Gravitino for admin
- **WHEN** a platform administrator with CATALOG_ADMIN role creates a table via Gravitino
- **THEN** Gravitino SHALL execute the corresponding DDL on the PostgreSQL backend and confirm success

#### Scenario: DDL rejected for read-only tenant role
- **WHEN** a tenant with READ_ONLY role attempts a DDL operation
- **THEN** Gravitino SHALL return HTTP 403 Forbidden
