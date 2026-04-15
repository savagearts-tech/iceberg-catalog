## ADDED Requirements

### Requirement: Gravitino Server deployment
The system SHALL deploy Apache Gravitino Server as the central metadata service, exposing a REST API on a configurable port (default 8090) and persisting metadata to a relational backend (PostgreSQL or embedded H2 for dev).

#### Scenario: Server startup with PostgreSQL backend
- **WHEN** Gravitino Server is started with a PostgreSQL JDBC backend configured
- **THEN** the server SHALL start within 60 seconds, expose `GET /api/version` returning HTTP 200, and persist metadata state across restarts

#### Scenario: Metalake creation
- **WHEN** a platform administrator sends `POST /api/metalakes` with a unique metalake name
- **THEN** Gravitino SHALL create the metalake and return HTTP 200 with the metalake entity

### Requirement: Catalog registration API
The system SHALL provide a REST API for registering, listing, and deleting Catalogs within a metalake.

#### Scenario: List registered catalogs
- **WHEN** a client sends `GET /api/metalakes/{metalake}/catalogs`
- **THEN** Gravitino SHALL return a list of all registered catalogs with their type, provider, and status

#### Scenario: Delete a catalog
- **WHEN** a platform administrator sends `DELETE /api/metalakes/{metalake}/catalogs/{catalog}`
- **THEN** Gravitino SHALL remove the catalog registration (metadata only, no data deletion) and return HTTP 200

### Requirement: Health and readiness endpoints
The Gravitino Server SHALL expose `/api/version` and a health endpoint for liveness/readiness probes in Kubernetes deployments.

#### Scenario: Health check passes
- **WHEN** the Kubernetes liveness probe calls `GET /api/version`
- **THEN** the server SHALL return HTTP 200 with the version JSON payload
