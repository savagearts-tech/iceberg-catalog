## ADDED Requirements

### Requirement: Run status query API
The system SHALL expose `GET /api/runs/{runId}` returning the current run status, including: `runId`, `appId`, `version`, `tenantId`, `status` (QUEUED / RUNNING / SUCCEEDED / FAILED / CANCELLED), `startedAt`, `completedAt`, and `statusReason`.

#### Scenario: Query status of a running instance
- **WHEN** a developer calls `GET /api/runs/{runId}` for a running Spark job
- **THEN** the response SHALL include `status: "RUNNING"`, `startedAt` timestamp, and the current K8s Pod name

#### Scenario: Query status of a failed run
- **WHEN** a run fails due to OOM
- **THEN** `GET /api/runs/{runId}` SHALL return `status: "FAILED"` and `statusReason: "OOM_KILLED"` with the container exit code

### Requirement: Real-time log streaming API
The system SHALL expose `GET /api/runs/{runId}/logs?follow=true` that streams the combined stdout and stderr of the application container in real time using Server-Sent Events (SSE) or chunked HTTP transfer encoding.

#### Scenario: Stream logs for a running application
- **WHEN** a developer calls `GET /api/runs/{runId}/logs?follow=true` while the app is running
- **THEN** new log lines SHALL be delivered to the client within 5 seconds of being written by the application

#### Scenario: Retrieve historical logs after completion
- **WHEN** a developer calls `GET /api/runs/{runId}/logs` (without `follow`) after a run completes
- **THEN** the full log output SHALL be returned, retained for at least 7 days after run completion

#### Scenario: Log access denied for cross-tenant run
- **WHEN** tenant-B calls `GET /api/runs/{runId}/logs` for a run belonging to tenant-A
- **THEN** the system SHALL return HTTP 403 Forbidden

### Requirement: Run resource metrics
The system SHALL expose `GET /api/runs/{runId}/metrics` returning a snapshot of the current resource utilization: CPU usage (millicores), memory usage (bytes), and for Spark applications, driver/executor counts and task progress.

#### Scenario: Metrics available during run
- **WHEN** a developer queries metrics for a `RUNNING` instance
- **THEN** the response SHALL include `cpuMillicores`, `memoryBytes` reflecting the last 30-second average from the Kubernetes metrics server

#### Scenario: Metrics not available for queued run
- **WHEN** a developer queries metrics for a `QUEUED` run
- **THEN** the system SHALL return HTTP 404 with reason `RUN_NOT_STARTED`

### Requirement: List runs with filtering
The system SHALL expose `GET /api/runs` supporting filters by `appId`, `status`, `startedAfter`, and `startedBefore`. Results SHALL be paginated with a default page size of 20.

#### Scenario: Filter by status
- **WHEN** a tenant calls `GET /api/runs?status=RUNNING`
- **THEN** only runs in `RUNNING` state belonging to the calling tenant SHALL be returned
