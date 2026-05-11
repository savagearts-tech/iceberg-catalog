package com.lakehouse.catalog.client.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link JsonLinePendingJournal}. Methods tagged {@code slow} scan tens of thousands of lines;
 * default {@code mvn test} skips them via {@code tests.excludeGroups}; run all with
 * {@code mvn -pl catalog-client test -Dtests.excludeGroups=}.
 */
class JsonLinePendingJournalTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("should_ReturnUncommittedOnly_afterMixedHistory")
    void should_ReturnUncommittedOnly_afterMixedHistory() throws IOException {
        Path journalDir = tempDir.resolve("j1");
        JsonLinePendingJournal journal = new JsonLinePendingJournal(journalDir, "t1", false);
        journal.appendPending(samplePending("a", "/x/1"));
        journal.appendPending(samplePending("b", "/x/2"));
        journal.appendCommitted(List.of("a"));

        List<PendingJournalRecord> open = journal.loadUncommitted();
        assertThat(open).hasSize(1);
        assertThat(open.get(0).entryId()).isEqualTo("b");
    }

    @Test
    @DisplayName("should_StreamReadLargeFile_withoutLoadingAllLinesIntoMemory")
    void should_StreamReadLargeFile_withoutLoadingAllLinesIntoMemory() throws IOException {
        Path journalDir = tempDir.resolve("j2");
        Files.createDirectories(journalDir);
        Path journalFile = journalDir.resolve(JsonLinePendingJournal.safeFileName("stream") + ".coalescing.journal.jsonl");
        try (BufferedWriter w = Files.newBufferedWriter(journalFile, StandardCharsets.UTF_8)) {
            for (int i = 0; i < 800; i++) {
                PendingJournalRecord committed = new PendingJournalRecord(
                        "old-" + i, JournalState.COMMITTED, null, null, null, null, null);
                w.write(MAPPER.writeValueAsString(committed));
                w.newLine();
            }
            for (int i = 0; i < 7; i++) {
                w.write(MAPPER.writeValueAsString(samplePending("open-" + i, "/open/" + i)));
                w.newLine();
            }
        }

        JsonLinePendingJournal journal = new JsonLinePendingJournal(journalDir, "stream", false);
        assertThat(journal.loadUncommitted()).hasSize(7);
    }

    @Test
    @Tag("slow")
    @DisplayName("should_ScanManyCommittedLines_thenReturnPendingTail")
    void should_ScanManyCommittedLines_thenReturnPendingTail() throws IOException {
        Path journalDir = tempDir.resolve("j_slow");
        Files.createDirectories(journalDir);
        Path journalFile = journalDir.resolve(JsonLinePendingJournal.safeFileName("slowtbl") + ".coalescing.journal.jsonl");
        try (BufferedWriter w = Files.newBufferedWriter(journalFile, StandardCharsets.UTF_8)) {
            for (int i = 0; i < 25_000; i++) {
                PendingJournalRecord committed = new PendingJournalRecord(
                        "hist-" + i, JournalState.COMMITTED, null, null, null, null, null);
                w.write(MAPPER.writeValueAsString(committed));
                w.newLine();
            }
            for (int i = 0; i < 12; i++) {
                w.write(MAPPER.writeValueAsString(samplePending("tail-" + i, "/tail/" + i)));
                w.newLine();
            }
        }

        JsonLinePendingJournal journal = new JsonLinePendingJournal(journalDir, "slowtbl", false);
        List<PendingJournalRecord> pending = journal.loadUncommitted();
        assertThat(pending).hasSize(12);
        assertThat(pending.get(0).path()).contains("/tail/0");
    }

    @Test
    @DisplayName("should_RotateToNewSegment_whenLineThresholdReached")
    void should_RotateToNewSegment_whenLineThresholdReached() throws IOException {
        Path journalDir = tempDir.resolve("j_rotate");
        JsonLinePendingJournal journal = new JsonLinePendingJournal(journalDir, "rot", false, 5);
        for (int i = 0; i < 12; i++) {
            journal.appendPending(samplePending("id-" + i, "/p/" + i));
        }
        try (Stream<Path> stream = Files.list(journalDir)) {
            List<Path> files = stream.collect(Collectors.toList());
            assertThat(files).hasSizeGreaterThanOrEqualTo(3);
        }
        List<PendingJournalRecord> pending = journal.loadUncommitted();
        assertThat(pending).hasSize(12);
    }

    @Test
    @DisplayName("should_MergeCommittedAcrossSegments_afterRotation")
    void should_MergeCommittedAcrossSegments_afterRotation() throws IOException {
        Path journalDir = tempDir.resolve("j_cross");
        JsonLinePendingJournal journal = new JsonLinePendingJournal(journalDir, "cross", false, 3);
        journal.appendPending(samplePending("a", "/1"));
        journal.appendPending(samplePending("b", "/2"));
        journal.appendPending(samplePending("c", "/3"));
        journal.appendCommitted(List.of("a"));

        List<PendingJournalRecord> open = journal.loadUncommitted();
        assertThat(open).extracting(PendingJournalRecord::entryId).containsExactlyInAnyOrder("b", "c");
    }

    @Test
    @DisplayName("should_BatchAppendCommitted_inSingleFileOpen")
    void should_BatchAppendCommitted_inSingleFileOpen() throws IOException {
        Path journalDir = tempDir.resolve("j3");
        JsonLinePendingJournal journal = new JsonLinePendingJournal(journalDir, "batch", false);
        journal.appendPending(samplePending("e1", "/p/1"));
        journal.appendPending(samplePending("e2", "/p/2"));
        journal.appendCommitted(List.of("e1", "e2"));

        assertThat(journal.loadUncommitted()).isEmpty();
    }

    private static PendingJournalRecord samplePending(String entryId, String path) {
        return new PendingJournalRecord(
                entryId, JournalState.PENDING, path, "parquet", 10L, 1L, null);
    }
}
