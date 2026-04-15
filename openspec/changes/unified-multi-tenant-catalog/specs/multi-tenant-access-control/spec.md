## ADDED Requirements

### Requirement: Tenant namespace provisioning API
The system SHALL expose a Tenant Management API (`POST /api/tenants`) that creates a Gravitino namespace, provisions a MinIO path prefix, generates tenant AccessKey credentials, and applies default ACL policies in a single atomic operation.

#### Scenario: New tenant onboarding
- **WHEN** a platform administrator calls `POST /api/tenants` with `{"tenantId": "acme", "displayName": "Acme Corp"}`
- **THEN** the system SHALL: (1) create Gravitino namespace `lakehouse.acme`, (2) create MinIO path `s3a://lakehouse/acme/`, (3) generate a tenant service account with scoped MinIO policy and write credentials to Secret Manager, (4) return `{ tenantId, namespace, credentialRef }` — **no plaintext credentials in response body**

#### Scenario: Duplicate tenant ID rejected
- **WHEN** a platform administrator attempts to create a tenant with an existing tenantId
- **THEN** the API SHALL return HTTP 409 Conflict with reason `TENANT_ALREADY_EXISTS`

### Requirement: Tenant sees only their authorized Lakehouse assets
The system SHALL enforce that a tenant user can only list and access Catalogs, Namespaces, and Tables for which they have been granted explicit permission.

#### Scenario: Tenant listing catalogs
- **WHEN** a tenant user calls `GET /api/metalakes/{metalake}/catalogs` with their auth token
- **THEN** the response SHALL include only catalogs the tenant has READ permission on

#### Scenario: Tenant listing tables in their namespace
- **WHEN** a tenant calls `GET /api/metalakes/{metalake}/catalogs/lakehouse/schemas/acme/tables`
- **THEN** the response SHALL include only tables within `acme` namespace

#### Scenario: Cross-tenant table access denied
- **WHEN** tenant-A's token is used to access `GET /api/.../schemas/tenant-B/tables/{table}`
- **THEN** Gravitino SHALL return HTTP 403 Forbidden

### Requirement: Role-based access within a tenant
Each tenant SHALL support internal role assignment: `ADMIN` (full CRUD on own namespace) and `VIEWER` (read-only access to own namespace).

#### Scenario: Tenant ADMIN creates a table
- **WHEN** a user with tenant ADMIN role creates an Iceberg table in their namespace
- **THEN** the operation SHALL succeed and the table SHALL be visible to all users in the tenant

#### Scenario: Tenant VIEWER cannot drop a table
- **WHEN** a user with tenant VIEWER role attempts to drop a table in their own namespace
- **THEN** the operation SHALL fail with HTTP 403 Forbidden

### Requirement: Tenant offboarding
The system SHALL support tenant deactivation that revokes access credentials and optionally archives or deletes tenant data, applied within a configurable grace period.

#### Scenario: Tenant deactivation
- **WHEN** a platform administrator calls `DELETE /api/tenants/{tenantId}` with `{"archiveData": true}`
- **THEN** the tenant's MinIO AccessKey SHALL be revoked, Gravitino ACL policies SHALL be removed, and data SHALL be moved to an archive prefix (`s3a://lakehouse/archived/{tenantId}/`)
