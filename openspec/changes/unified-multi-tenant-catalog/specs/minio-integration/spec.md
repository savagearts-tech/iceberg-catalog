## ADDED Requirements

### Requirement: MinIO S3 backend configuration
The system SHALL configure MinIO as the S3-compatible object storage backend for Iceberg data files, using the `s3a://` URI scheme with MinIO endpoint, access key, and secret key properties.

#### Scenario: MinIO credential validation on catalog creation
- **WHEN** a platform administrator registers the `lakehouse` Iceberg catalog with MinIO credentials
- **THEN** Gravitino SHALL validate connectivity to the MinIO endpoint and fail-fast with a descriptive error if credentials are invalid

#### Scenario: Data file written to MinIO
- **WHEN** a Spark job writes a new Iceberg table partition
- **THEN** the data files SHALL be persisted to `s3a://lakehouse/<tenant-id>/<table-path>/` on MinIO

### Requirement: Tenant bucket prefix isolation
Each tenant's Iceberg data files SHALL be stored under a dedicated MinIO path prefix `s3a://lakehouse/<tenant-id>/` to provide storage-level isolation.

#### Scenario: Cross-tenant path isolation
- **WHEN** tenant-A writes data to their table
- **THEN** the file path SHALL contain `lakehouse/tenant-A/` prefix and SHALL NOT overlap with `lakehouse/tenant-B/` paths

### Requirement: MinIO tenant AccessKey lifecycle
The system SHALL support provisioning and revoking tenant-specific MinIO AccessKey/SecretKey pairs through the MinIO Admin API.

#### Scenario: Tenant AccessKey provisioning
- **WHEN** a new tenant namespace is created
- **THEN** the system SHALL programmatically create a MinIO service account scoped to the tenant's bucket prefix

#### Scenario: AccessKey revocation on tenant offboarding
- **WHEN** a tenant is deactivated
- **THEN** the system SHALL revoke the tenant's MinIO AccessKey within 60 seconds

### Requirement: MinIO AccessKey stored in Secret Manager
Tenant MinIO AccessKey and SecretKey SHALL NOT be returned in plaintext in the Tenant Onboarding API response body. They SHALL be written to the Secret Manager (Vault or cloud-native equivalent) and retrieved via a separately issued one-time credential retrieval token.

#### Scenario: Onboarding response contains no plaintext credentials
- **WHEN** `POST /api/tenants` succeeds
- **THEN** the response body SHALL contain `{ tenantId, namespace, credentialRef }` and SHALL NOT contain `accessKey` or `secretKey` fields

#### Scenario: Credentials retrieved via one-time token
- **WHEN** a tenant administrator calls `GET /api/tenants/{tenantId}/credentials?token=<one-time-token>` within the token's 15-minute validity window
- **THEN** the system SHALL return the tenant's AccessKey and SecretKey from Secret Manager and mark the token as consumed

#### Scenario: Expired or reused token rejected
- **WHEN** a tenant calls the credential retrieval endpoint with an expired or already-consumed token
- **THEN** the system SHALL return HTTP 401 Unauthorized with reason `CREDENTIAL_TOKEN_INVALID`

### Requirement: MinIO Bucket Versioning for Iceberg delete files
The `lakehouse` bucket SHALL have Object Versioning enabled to correctly support Iceberg v2 position delete files and equality delete files, preventing premature deletion of referenced data objects during concurrent compaction operations.

#### Scenario: Bucket versioning enabled on provisioning
- **WHEN** the `lakehouse` bucket is created during platform setup
- **THEN** MinIO Bucket Versioning SHALL be enabled on the bucket

#### Scenario: Iceberg position delete file retained
- **WHEN** an Iceberg compaction job rewrites data files and removes the original position delete files from the snapshot
- **THEN** the referenced delete file objects SHALL still be accessible via their version ID until the next successful `expire_snapshots` run confirms they are no longer referenced
