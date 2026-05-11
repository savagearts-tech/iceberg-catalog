package com.lakehouse.catalog.client.writer;

import com.lakehouse.catalog.client.IcebergCatalogClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Batches multiple staged {@link DataFile}s into a single Iceberg {@link AppendFiles#commit()} and
 * persists pending paths to a JSONL journal so uncommitted files can be replayed after a crash.
 *
 * <p><strong>Concurrency:</strong> this class is guarded by a {@link ReentrantLock}. Do not run
 * multiple {@link CoalescingAppendWriter} instances against the same table and journal directory
 * without external coordination; commits may conflict or duplicate.</p>
 *
 * <p><strong>Metrics and recovery:</strong> the journal stores path, format, sizes, counts, and a
 * Hive-style {@code partitionPath} when the table is partitioned. On replay, {@link DataFile}s are
 * rebuilt with {@link DataFiles#builder} using minimal column metrics (defaults). If downstream
 * planning depends on rich Parquet statistics, either keep the writer process alive until commit
 * or extend the journal in a future iteration to persist full metrics.</p>
 *
 * <p><strong>Orphans and housekeeping:</strong> callers should stage files only after the object
 * store upload is complete so the journal always refers to readable objects. Retained Iceberg
 * snapshots may reference data files; use table maintenance (for example {@code expire_snapshots})
 * according to your retention policy.</p>
 *
 * <p><strong>Journal growth:</strong> the built-in {@link JsonLinePendingJournal} never removes lines;
 * each successful commit appends COMMITTED markers, so the log file grows monotonically. Recovery
 * scans the full log O(total lines). Long-running or very high-frequency staging requires a rotation
 * or compaction strategy, a different {@link PendingDataFileJournal}, or external durable state.</p>
 *
 * <p>Opens with a single {@link IcebergCatalogClient#loadTable(String)}; the same {@link Table}
 * instance is reused for staging, recovery, and commits. Metadata is refreshed via {@link Table#refresh()}
 * before each append commit so concurrent catalog updates are visible.</p>
 *
 * @see IcebergCatalogClient#loadTable(String)
 */
@Slf4j
public final class CoalescingAppendWriter implements Closeable {

    private final IcebergCatalogClient client;
    private final String tableName;
    /** Loaded once at construction; refreshed before commits. */
    private final Table table;
    private final PendingDataFileJournal journal;
    private final CoalescingAppendWriterConfig config;

    private final ReentrantLock lock = new ReentrantLock();
    private final List<Staged> staged = new ArrayList<>();
    private long pendingBytes;
    private long lastFlushNanos;
    private boolean closed;

    private CoalescingAppendWriter(
            IcebergCatalogClient client,
            String tableName,
            PendingDataFileJournal journal,
            CoalescingAppendWriterConfig config) throws IOException {
        this.client = Objects.requireNonNull(client, "client");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.journal = Objects.requireNonNull(journal, "journal");
        this.config = Objects.requireNonNull(config, "config");
        this.table = this.client.loadTable(this.tableName);
        this.lastFlushNanos = System.nanoTime();
        recoverFromJournalOnOpen();
    }

    /**
     * Opens a writer for {@code tableName}, using {@code journalDirectory} for the append-only JSONL log
     * ({@link JsonLinePendingJournal}). Replays any journal entries that are still {@link JournalState#PENDING}
     * and attempts one flush. For journal size and recovery cost, see {@link JsonLinePendingJournal}.
     */
    public static CoalescingAppendWriter open(
            IcebergCatalogClient client,
            String tableName,
            Path journalDirectory,
            CoalescingAppendWriterConfig config) throws IOException {
        PendingDataFileJournal journal =
                new JsonLinePendingJournal(
                        journalDirectory, tableName, config.fsyncJournal(), config.maxJournalLinesPerFile());
        return new CoalescingAppendWriter(client, tableName, journal, config);
    }

    /**
     * Records a finished data file in the journal (durable first) and the in-memory pending batch,
     * then flushes if thresholds are met.
     */
    public void stage(DataFile dataFile) throws IOException {
        Objects.requireNonNull(dataFile, "dataFile");
        lock.lock();
        try {
            ensureOpen();
            String entryId = UUID.randomUUID().toString();
            PendingJournalRecord record = toJournalRecord(table, entryId, dataFile);
            journal.appendPending(record);
            staged.add(new Staged(entryId, dataFile));
            pendingBytes += dataFile.fileSizeInBytes();
            if (shouldFlush()) {
                flushLocked();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Commits all pending staged files in one Iceberg append transaction (after {@link Table#refresh()}).
     */
    public void flush() throws IOException {
        lock.lock();
        try {
            ensureOpen();
            flushLocked();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reloads uncommitted journal lines and attempts to commit them (intended for explicit replay;
     * {@link #open} already runs recovery once).
     */
    public void recover() throws IOException {
        lock.lock();
        try {
            ensureOpen();
            List<PendingJournalRecord> pending = journal.loadUncommitted();
            if (pending.isEmpty()) {
                return;
            }
            Set<String> alreadyStaged = new HashSet<>();
            for (Staged s : staged) {
                alreadyStaged.add(s.entryId);
            }
            for (PendingJournalRecord r : pending) {
                if (alreadyStaged.contains(r.entryId())) {
                    continue;
                }
                DataFile rebuilt = rebuildDataFile(table, r);
                staged.add(new Staged(r.entryId(), rebuilt));
                pendingBytes += rebuilt.fileSizeInBytes();
            }
            flushLocked();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (config.flushOnClose() && !staged.isEmpty()) {
                flushLocked();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Loads PENDING journal rows from disk and attempts one commit (constructor path; lock is internal). */
    private void recoverFromJournalOnOpen() throws IOException {
        lock.lock();
        try {
            List<PendingJournalRecord> pending = journal.loadUncommitted();
            for (PendingJournalRecord r : pending) {
                DataFile rebuilt = rebuildDataFile(table, r);
                staged.add(new Staged(r.entryId(), rebuilt));
                pendingBytes += rebuilt.fileSizeInBytes();
            }
            if (!staged.isEmpty()) {
                flushLocked();
            }
        } finally {
            lock.unlock();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("CoalescingAppendWriter is closed");
        }
    }

    private boolean shouldFlush() {
        if (staged.isEmpty()) {
            return false;
        }
        if (staged.size() >= config.maxPendingFiles()) {
            return true;
        }
        if (pendingBytes >= config.maxPendingBytes()) {
            return true;
        }
        long elapsedNanos = System.nanoTime() - lastFlushNanos;
        return elapsedNanos >= config.maxFlushInterval().toNanos();
    }

    private void flushLocked() throws IOException {
        if (staged.isEmpty()) {
            return;
        }
        int attempt = 0;
        while (!staged.isEmpty()) {
            attempt++;
            try {
                table.refresh();
                AppendFiles append = table.newAppend();
                for (Staged s : staged) {
                    append.appendFile(s.dataFile);
                }
                append.commit();
                List<String> ids = new ArrayList<>(staged.size());
                for (Staged s : staged) {
                    ids.add(s.entryId);
                }
                journal.appendCommitted(ids);
                staged.clear();
                pendingBytes = 0;
                lastFlushNanos = System.nanoTime();
                log.debug("Committed {} data files to table {}", ids.size(), tableName);
                return;
            } catch (Exception e) {
                log.warn("Append commit failed (attempt {}/{}): {}", attempt, config.maxCommitRetries(), e.getMessage());
                if (attempt >= config.maxCommitRetries()) {
                    throw new IOException("Iceberg append commit failed after " + attempt + " attempts", e);
                }
                boolean removed = removeAlreadyCommittedPaths();
                if (!removed) {
                    backoffSleep(attempt);
                }
            }
        }
    }

    private static void backoffSleep(int attempt) throws IOException {
        try {
            Thread.sleep(Math.min(100L * attempt, 1_000L));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during append retry backoff", ie);
        }
    }

    /**
     * Drops staged files whose paths already appear in the current table state and marks them committed in the journal.
     *
     * @return true if anything was removed (caller may retry immediately)
     */
    private boolean removeAlreadyCommittedPaths() throws IOException {
        table.refresh();
        Set<String> existingPaths = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                existingPaths.add(task.file().path().toString());
            }
        }
        boolean removed = false;
        Iterator<Staged> it = staged.iterator();
        List<String> toMarkCommitted = new ArrayList<>();
        while (it.hasNext()) {
            Staged s = it.next();
            if (existingPaths.contains(s.dataFile.path().toString())) {
                toMarkCommitted.add(s.entryId);
                pendingBytes -= s.dataFile.fileSizeInBytes();
                it.remove();
                removed = true;
            }
        }
        if (!toMarkCommitted.isEmpty()) {
            journal.appendCommitted(toMarkCommitted);
        }
        return removed;
    }

    private static PendingJournalRecord toJournalRecord(Table table, String entryId, DataFile df) {
        String partitionPath = "";
        if (table.spec().isPartitioned()) {
            partitionPath = table.spec().partitionToPath(df.partition());
        }
        return new PendingJournalRecord(
                entryId,
                JournalState.PENDING,
                df.path().toString(),
                df.format().toString().toLowerCase(Locale.ROOT),
                df.fileSizeInBytes(),
                df.recordCount(),
                partitionPath.isEmpty() ? null : partitionPath);
    }

    static DataFile rebuildDataFile(Table table, PendingJournalRecord r) {
        DataFiles.Builder builder = DataFiles.builder(table.spec())
                .withPath(r.path())
                .withFormat(parseFormat(r.format()))
                .withFileSizeInBytes(r.fileSizeInBytes())
                .withRecordCount(r.recordCount());
        String partitionPath = r.partitionPath();
        if (partitionPath != null && !partitionPath.isBlank()) {
            builder.withPartitionPath(partitionPath);
        }
        return builder.build();
    }

    private static FileFormat parseFormat(String format) {
        if (format == null || format.isBlank()) {
            return FileFormat.PARQUET;
        }
        return FileFormat.fromString(format.toLowerCase(Locale.ROOT));
    }

    private record Staged(String entryId, DataFile dataFile) {
    }
}
