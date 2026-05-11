package com.lakehouse.catalog.client.writer;

import java.time.Duration;
import java.util.Objects;

/**
 * Tuning for {@link CoalescingAppendWriter}: when to flush pending {@link org.apache.iceberg.DataFile}s
 * in a single Iceberg commit, and how the on-disk journal behaves.
 */
public final class CoalescingAppendWriterConfig {

    public static final int DEFAULT_MAX_PENDING_FILES = 100;
    public static final long DEFAULT_MAX_PENDING_BYTES = 512L * 1024 * 1024;
    public static final Duration DEFAULT_MAX_FLUSH_INTERVAL = Duration.ofMinutes(1);
    public static final int DEFAULT_MAX_COMMIT_RETRIES = 5;
    /** 0 = journal rotation disabled ({@link JsonLinePendingJournal} keeps one active file). */
    public static final int DEFAULT_MAX_JOURNAL_LINES_PER_FILE = 0;

    private final int maxPendingFiles;
    private final long maxPendingBytes;
    private final Duration maxFlushInterval;
    private final int maxCommitRetries;
    private final boolean flushOnClose;
    private final boolean fsyncJournal;
    private final int maxJournalLinesPerFile;

    private CoalescingAppendWriterConfig(Builder builder) {
        this.maxPendingFiles = builder.maxPendingFiles;
        this.maxPendingBytes = builder.maxPendingBytes;
        this.maxFlushInterval = builder.maxFlushInterval;
        this.maxCommitRetries = builder.maxCommitRetries;
        this.flushOnClose = builder.flushOnClose;
        this.fsyncJournal = builder.fsyncJournal;
        this.maxJournalLinesPerFile = builder.maxJournalLinesPerFile;
    }

    public int maxPendingFiles() {
        return maxPendingFiles;
    }

    public long maxPendingBytes() {
        return maxPendingBytes;
    }

    public Duration maxFlushInterval() {
        return maxFlushInterval;
    }

    public int maxCommitRetries() {
        return maxCommitRetries;
    }

    public boolean flushOnClose() {
        return flushOnClose;
    }

    public boolean fsyncJournal() {
        return fsyncJournal;
    }

    /**
     * When positive, {@link JsonLinePendingJournal} renames the active segment after this many lines
     * are written to it, then continues in a new empty file. Zero disables rotation.
     */
    public int maxJournalLinesPerFile() {
        return maxJournalLinesPerFile;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static CoalescingAppendWriterConfig defaultConfig() {
        return builder().build();
    }

    public static final class Builder {
        private int maxPendingFiles = DEFAULT_MAX_PENDING_FILES;
        private long maxPendingBytes = DEFAULT_MAX_PENDING_BYTES;
        private Duration maxFlushInterval = DEFAULT_MAX_FLUSH_INTERVAL;
        private int maxCommitRetries = DEFAULT_MAX_COMMIT_RETRIES;
        private boolean flushOnClose = true;
        private boolean fsyncJournal = true;
        private int maxJournalLinesPerFile = DEFAULT_MAX_JOURNAL_LINES_PER_FILE;

        public Builder maxPendingFiles(int value) {
            if (value < 1) {
                throw new IllegalArgumentException("maxPendingFiles must be >= 1");
            }
            this.maxPendingFiles = value;
            return this;
        }

        public Builder maxPendingBytes(long value) {
            if (value < 1) {
                throw new IllegalArgumentException("maxPendingBytes must be >= 1");
            }
            this.maxPendingBytes = value;
            return this;
        }

        public Builder maxFlushInterval(Duration value) {
            this.maxFlushInterval = Objects.requireNonNull(value, "maxFlushInterval");
            if (value.isNegative() || value.isZero()) {
                throw new IllegalArgumentException("maxFlushInterval must be positive");
            }
            return this;
        }

        public Builder maxCommitRetries(int value) {
            if (value < 1) {
                throw new IllegalArgumentException("maxCommitRetries must be >= 1");
            }
            this.maxCommitRetries = value;
            return this;
        }

        public Builder flushOnClose(boolean value) {
            this.flushOnClose = value;
            return this;
        }

        public Builder fsyncJournal(boolean value) {
            this.fsyncJournal = value;
            return this;
        }

        /**
         * @param value max lines per active journal segment before rotating; 0 disables rotation
         */
        public Builder maxJournalLinesPerFile(int value) {
            if (value < 0) {
                throw new IllegalArgumentException("maxJournalLinesPerFile must be >= 0");
            }
            this.maxJournalLinesPerFile = value;
            return this;
        }

        public CoalescingAppendWriterConfig build() {
            return new CoalescingAppendWriterConfig(this);
        }
    }
}
