## ADDED Requirements

### Requirement: Application run submission API
The system SHALL expose `POST /api/runs` to submit an application for execution, accepting `appId`, `version`, and optional runtime parameters. Each run SHALL be assigned a unique `runId`.

#### Scenario: Submit a run for a registered app
- **WHEN** a tenant developer calls `POST /api/runs` with `{"appId": "my-etl", "version": "v1", "params": {"date": "2026-04-08"}}`
- **THEN** the system SHALL create a run entry with `status: "QUEUED"` and return `{ runId, status, queuePosition }`

#### Scenario: Run rejected for unregistered app
- **WHEN** a developer submits a run for an `appId` that does not exist in the registry
- **THEN** the system SHALL return HTTP 404 Not Found with reason `APP_NOT_FOUND`

### Requirement: Tenant resource quota enforcement
The system SHALL check the tenant's remaining ResourceQuota before transitioning a run from `QUEUED` to `RUNNING`. If quota is insufficient, the run SHALL remain in `QUEUED` state with a `QUOTA_EXCEEDED` status reason.

#### Scenario: Run queued due to quota exhaustion
- **WHEN** a tenant submits a run that would exceed their CPU or memory quota
- **THEN** the run SHALL enter `QUEUED` state and the response SHALL include `statusReason: "QUOTA_EXCEEDED"` and the current queue position

#### Scenario: Queued run promoted when quota becomes available
- **WHEN** a previously running instance completes and frees quota
- **THEN** the next queued run for that tenant SHALL be promoted to `RUNNING` within 30 seconds

### Requirement: FIFO scheduling within tenant queue
Within a single tenant's run queue, the system SHALL dispatch runs in FIFO order based on submission time.

#### Scenario: First submitted run dispatched first
- **WHEN** tenant-A has two queued runs submitted at T1 and T2 (T1 < T2)
- **THEN** the run submitted at T1 SHALL be dispatched before T2 when quota becomes available

### Requirement: Run cancellation
The system SHALL support cancelling a run via `DELETE /api/runs/{runId}`. A `QUEUED` run SHALL be removed immediately; a `RUNNING` run SHALL receive a SIGTERM signal and transition to `CANCELLED` within 30 seconds.

#### Scenario: Cancel a queued run
- **WHEN** a developer calls `DELETE /api/runs/{runId}` for a run in `QUEUED` state
- **THEN** the run SHALL transition to `CANCELLED` immediately and no K8s resources SHALL be created

#### Scenario: Cancel a running Pod-based app
- **WHEN** a developer cancels a `RUNNING` Docker-type run
- **THEN** the Pod SHALL receive SIGTERM, and if it does not exit within 30 seconds, SIGKILL SHALL be sent; the run SHALL be recorded as `CANCELLED`
