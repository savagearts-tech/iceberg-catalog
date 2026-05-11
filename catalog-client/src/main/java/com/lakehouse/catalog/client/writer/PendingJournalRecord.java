package com.lakehouse.catalog.client.writer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One JSON line in the append-only journal. {@link JournalState#COMMITTED} lines
 * only require {@link #entryId} and {@link #state}; other fields may be null.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class PendingJournalRecord {

    @JsonProperty("entryId")
    private String entryId;
    @JsonProperty("state")
    private JournalState state;
    @JsonProperty("path")
    private String path;
    @JsonProperty("format")
    private String format;
    @JsonProperty("fileSizeInBytes")
    private Long fileSizeInBytes;
    @JsonProperty("recordCount")
    private Long recordCount;
    /** Hive-style partition path segment, e.g. {@code day=2024-01-01}; empty for unpartitioned. */
    @JsonProperty("partitionPath")
    private String partitionPath;

    public PendingJournalRecord() {
    }

    public PendingJournalRecord(
            String entryId,
            JournalState state,
            String path,
            String format,
            Long fileSizeInBytes,
            Long recordCount,
            String partitionPath) {
        this.entryId = entryId;
        this.state = state;
        this.path = path;
        this.format = format;
        this.fileSizeInBytes = fileSizeInBytes;
        this.recordCount = recordCount;
        this.partitionPath = partitionPath;
    }

    public String entryId() {
        return entryId;
    }

    public void setEntryId(String entryId) {
        this.entryId = entryId;
    }

    public JournalState state() {
        return state;
    }

    public void setState(JournalState state) {
        this.state = state;
    }

    public String path() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String format() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public Long fileSizeInBytes() {
        return fileSizeInBytes;
    }

    public void setFileSizeInBytes(Long fileSizeInBytes) {
        this.fileSizeInBytes = fileSizeInBytes;
    }

    public Long recordCount() {
        return recordCount;
    }

    public void setRecordCount(Long recordCount) {
        this.recordCount = recordCount;
    }

    public String partitionPath() {
        return partitionPath;
    }

    public void setPartitionPath(String partitionPath) {
        this.partitionPath = partitionPath;
    }
}
