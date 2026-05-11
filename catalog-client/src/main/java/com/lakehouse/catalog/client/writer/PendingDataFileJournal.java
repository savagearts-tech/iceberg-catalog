package com.lakehouse.catalog.client.writer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Persists staged data files so they can be re-committed after process restart.
 *
 * <p><strong>Operational contract:</strong> implementations differ in durability and scalability.
 * The reference {@link JsonLinePendingJournal} is correct and simple for bounded workloads; for
 * unbounded append volume, replace this interface with a store that supports compaction or bounded
 * recovery scans.</p>
 */
public interface PendingDataFileJournal {

    /**
     * Append a pending entry (must be durable before returning if journal fsync is enabled).
     */
    void appendPending(PendingJournalRecord record) throws IOException;

    /**
     * Append commit markers for the given entry ids (typically after a successful Iceberg commit).
     */
    void appendCommitted(Collection<String> entryIds) throws IOException;

    /**
     * Entries that were staged but never marked committed, in journal order (for replay).
     *
     * <p><strong>Complexity:</strong> for {@link JsonLinePendingJournal}, cost is O(N) in total journal
     * lines over the lifetime of the file (every line is read once per call). Memory should stay
     * O(uncommitted distinct entryIds + committed id set), not O(file size), when implemented with
     * streaming I/O.</p>
     */
    List<PendingJournalRecord> loadUncommitted() throws IOException;
}
