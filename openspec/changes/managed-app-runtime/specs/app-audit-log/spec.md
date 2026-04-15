## ADDED Requirements

### Requirement: Audit record created for every run lifecycle event
The system SHALL create an audit log entry for each of the following run lifecycle transitions: SUBMITTED, QUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED. Each entry SHALL record: `runId`, `appId`, `appVersion`, `tenantId`, `triggeredBy` (user or system), `event`, `timestamp`, `resourceUsage` (CPU seconds, memory-seconds), and `exitCode` (if applicable).

#### Scenario: Audit entry on run submission
- **WHEN** a developer submits a run via `POST /api/runs`
- **THEN** an audit entry with `event: "SUBMITTED"`, `triggeredBy: <userId>`, and `timestamp` SHALL be created within 1 second

#### Scenario: Audit entry on run completion with resource usage
- **WHEN** a run transitions to `SUCCEEDED`
- **THEN** an audit entry SHALL include `resourceUsage.cpuSeconds` and `resourceUsage.memorySeconds` computed from the K8s metrics data collected during the run

### Requirement: Audit logs written to platform PostgreSQL
All audit log entries SHALL be durably written to the platform PostgreSQL database table `app_run_audit_log` for platform administrator queries and compliance reporting.

#### Scenario: Platform admin queries audit log by tenant
- **WHEN** a platform administrator calls `GET /api/audit-logs?tenantId=acme&startDate=2026-04-01`
- **THEN** all audit events for tenant `acme` within the specified date range SHALL be returned in descending timestamp order

#### Scenario: Audit log retained for minimum 90 days
- **WHEN** an audit entry is created
- **THEN** the entry SHALL be retained in PostgreSQL for at least 90 days before becoming eligible for archival

### Requirement: Audit logs written to tenant Iceberg audit table
In addition to PostgreSQL, audit log entries SHALL be asynchronously written to the tenant's dedicated Iceberg audit table (`lakehouse.<tenantId>.platform_app_audit`) enabling self-service analysis via Spark/SQL.

#### Scenario: Tenant queries own audit data via Spark
- **WHEN** a tenant's Spark job queries `SELECT * FROM lakehouse.acme.platform_app_audit WHERE event = 'FAILED'`
- **THEN** the query SHALL return all failed run events for tenant `acme`

#### Scenario: Iceberg audit write failure does not block run
- **WHEN** the Iceberg audit write fails (e.g., MinIO unreachable)
- **THEN** the run SHALL continue normally; the failed audit entry SHALL be retried asynchronously up to 3 times and written to a dead-letter queue if all retries fail

### Requirement: Audit log immutability
Audit log entries SHALL be immutable once written. No API SHALL permit modifying or deleting individual audit entries. Archival (retirement to cold storage after 90 days) is permitted but SHALL preserve the original data.

#### Scenario: Audit entry modification rejected
- **WHEN** any API call attempts to update or delete a specific audit entry
- **THEN** the system SHALL return HTTP 405 Method Not Allowed
