/**
 * Iceberg append helpers: coalescing multiple {@link org.apache.iceberg.DataFile}s into fewer metadata commits
 * with an optional on-disk journal for crash recovery.
 *
 * <p>Prefer a single {@link com.lakehouse.catalog.client.writer.CoalescingAppendWriter} per physical table;
 * multiple writers against the same table require external locking or leadership.</p>
 *
 * <p><strong>Journal sustainability:</strong> the default {@link com.lakehouse.catalog.client.writer.JsonLinePendingJournal}
 * is append-only; optional {@link com.lakehouse.catalog.client.writer.CoalescingAppendWriterConfig#maxJournalLinesPerFile()}
 * rotates the active segment after a fixed line count. Total history across segments still grows until compact or archive.
 * {@link com.lakehouse.catalog.client.writer.PendingDataFileJournal#loadUncommitted()} scans the entire file
 * (linear time and streaming I/O, bounded memory per line, not whole-file buffer). Production deployments that
 * run for a long time or stage at very high frequency should plan log rotation or compaction, swap in another
 * {@link com.lakehouse.catalog.client.writer.PendingDataFileJournal} implementation (for example SQLite or an
 * external durable queue), or accept operational truncation when safe. See {@link
 * com.lakehouse.catalog.client.writer.JsonLinePendingJournal} class documentation for evolution options.</p>
 */
package com.lakehouse.catalog.client.writer;
