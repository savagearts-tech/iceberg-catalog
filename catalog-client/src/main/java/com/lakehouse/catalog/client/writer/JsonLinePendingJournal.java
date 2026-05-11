package com.lakehouse.catalog.client.writer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Append-only JSON lines journal. Each {@code stage} writes a {@link JournalState#PENDING} record;
 * after a successful Iceberg commit, {@link JournalState#COMMITTED} markers are appended for each entry id.
 *
 * <p><strong>Rotation:</strong> when {@code maxJournalLinesPerFile} is positive, the active file
 * {@code &lt;base&gt;.jsonl} is renamed to {@code &lt;base&gt;.&lt;seq&gt;.jsonl} after that many lines have been
 * written to the current segment, then a new empty active file is created. {@link #loadUncommitted()} reads
 * numbered segments in ascending order, then the active file, and merges state as one timeline.</p>
 *
 * <p><strong>Limits (read before production):</strong></p>
 * <ul>
 *   <li>Total data still grows across segments; rotation caps single-file size, not total history.</li>
 *   <li>{@link #loadUncommitted()} is O(total lines across all segments) time, streamed line-by-line.</li>
 *   <li>Optional per-append {@code fsync} adds latency; very high append rates need an explicit durability policy.</li>
 * </ul>
 */
public final class JsonLinePendingJournal implements PendingDataFileJournal {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path journalDirectory;
    /** Filename stem without trailing {@code .jsonl}, e.g. {@code tbl.coalescing.journal}. */
    private final String journalBaseFileName;
    private final Path activeJournalFile;
    private final boolean fsync;
    private final int maxJournalLinesPerFile;

    private int linesInCurrentSegment;

    public JsonLinePendingJournal(Path journalDirectory, String tableName, boolean fsyncJournal) throws IOException {
        this(journalDirectory, tableName, fsyncJournal, CoalescingAppendWriterConfig.DEFAULT_MAX_JOURNAL_LINES_PER_FILE);
    }

    /**
     * @param maxJournalLinesPerFile when {@code > 0}, rotate the active file after this many lines per segment; {@code 0} disables
     */
    public JsonLinePendingJournal(
            Path journalDirectory,
            String tableName,
            boolean fsyncJournal,
            int maxJournalLinesPerFile) throws IOException {
        Files.createDirectories(journalDirectory);
        this.journalDirectory = journalDirectory;
        this.journalBaseFileName = safeFileName(tableName) + ".coalescing.journal";
        this.activeJournalFile = journalDirectory.resolve(journalBaseFileName + ".jsonl");
        this.fsync = fsyncJournal;
        this.maxJournalLinesPerFile = maxJournalLinesPerFile;

        if (maxJournalLinesPerFile > 0 && Files.exists(activeJournalFile)) {
            int existing = countLines(activeJournalFile);
            if (existing >= maxJournalLinesPerFile) {
                rotateToNewSegment();
                this.linesInCurrentSegment = 0;
            } else {
                this.linesInCurrentSegment = existing;
            }
        } else {
            this.linesInCurrentSegment = 0;
        }
    }

    static String safeFileName(String tableName) {
        return tableName.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private String activeFileName() {
        return journalBaseFileName + ".jsonl";
    }

    private boolean isRotatedSegmentFileName(String name) {
        if (!name.endsWith(".jsonl")) {
            return false;
        }
        if (name.equals(activeFileName())) {
            return false;
        }
        String prefix = journalBaseFileName + ".";
        if (!name.startsWith(prefix)) {
            return false;
        }
        String middle = name.substring(prefix.length(), name.length() - ".jsonl".length());
        return !middle.isEmpty() && middle.chars().allMatch(Character::isDigit);
    }

    private long sequenceFromRotatedFileName(String name) {
        String prefix = journalBaseFileName + ".";
        String middle = name.substring(prefix.length(), name.length() - ".jsonl".length());
        return Long.parseLong(middle);
    }

    @Override
    public synchronized void appendPending(PendingJournalRecord record) throws IOException {
        appendLines(MAPPER.writeValueAsString(record) + "\n", 1);
    }

    @Override
    public synchronized void appendCommitted(Collection<String> entryIds) throws IOException {
        if (entryIds == null || entryIds.isEmpty()) {
            return;
        }
        StringBuilder batch = new StringBuilder(entryIds.size() * 64);
        for (String entryId : entryIds) {
            PendingJournalRecord marker = new PendingJournalRecord(
                    entryId, JournalState.COMMITTED, null, null, null, null, null);
            batch.append(MAPPER.writeValueAsString(marker)).append('\n');
        }
        appendLines(batch.toString(), entryIds.size());
    }

    private void appendLines(String utf8Payload, int lineCount) throws IOException {
        byte[] bytes = utf8Payload.getBytes(StandardCharsets.UTF_8);
        try (FileOutputStream fos = new FileOutputStream(activeJournalFile.toFile(), true)) {
            fos.write(bytes);
            fos.flush();
            if (fsync) {
                fos.getFD().sync();
            }
        }
        linesInCurrentSegment += lineCount;
        maybeRotateAfterAppend();
    }

    private void maybeRotateAfterAppend() throws IOException {
        if (maxJournalLinesPerFile <= 0 || linesInCurrentSegment < maxJournalLinesPerFile) {
            return;
        }
        rotateToNewSegment();
        linesInCurrentSegment = 0;
    }

    private void rotateToNewSegment() throws IOException {
        if (!Files.exists(activeJournalFile)) {
            return;
        }
        long nextSeq = maxRotatedSequenceNumber() + 1;
        Path target = journalDirectory.resolve(journalBaseFileName + "." + nextSeq + ".jsonl");
        while (Files.exists(target)) {
            nextSeq++;
            target = journalDirectory.resolve(journalBaseFileName + "." + nextSeq + ".jsonl");
        }
        Files.move(activeJournalFile, target);
        Files.createFile(activeJournalFile);
    }

    private long maxRotatedSequenceNumber() throws IOException {
        long max = 0;
        try (Stream<Path> stream = Files.list(journalDirectory)) {
            for (Path p : stream.collect(Collectors.toList())) {
                String name = p.getFileName().toString();
                if (isRotatedSegmentFileName(name)) {
                    max = Math.max(max, sequenceFromRotatedFileName(name));
                }
            }
        }
        return max;
    }

    private static int countLines(Path file) throws IOException {
        int n = 0;
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            while (reader.readLine() != null) {
                n++;
            }
        }
        return n;
    }

    /**
     * Rotated segments {@code base.N.jsonl} in ascending {@code N}, then the active {@code base.jsonl}.
     */
    private List<Path> segmentPathsOldestFirst() throws IOException {
        List<Path> rotated = new ArrayList<>();
        try (Stream<Path> stream = Files.list(journalDirectory)) {
            for (Path p : stream.collect(Collectors.toList())) {
                String name = p.getFileName().toString();
                if (isRotatedSegmentFileName(name)) {
                    rotated.add(p);
                }
            }
        }
        rotated.sort(Comparator.comparingLong(p -> sequenceFromRotatedFileName(p.getFileName().toString())));
        List<Path> ordered = new ArrayList<>(rotated);
        if (Files.exists(activeJournalFile)) {
            ordered.add(activeJournalFile);
        }
        return ordered;
    }

    @Override
    public synchronized List<PendingJournalRecord> loadUncommitted() throws IOException {
        List<Path> segments = segmentPathsOldestFirst();
        if (segments.isEmpty()) {
            return List.of();
        }
        Map<String, PendingJournalRecord> lastPendingById = new LinkedHashMap<>();
        Set<String> committed = new HashSet<>();
        for (Path segment : segments) {
            try (BufferedReader reader = Files.newBufferedReader(segment, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    PendingJournalRecord record = MAPPER.readValue(line, PendingJournalRecord.class);
                    if (record.state() == JournalState.COMMITTED) {
                        committed.add(record.entryId());
                    } else if (record.state() == JournalState.PENDING && record.path() != null) {
                        lastPendingById.put(record.entryId(), record);
                    }
                }
            }
        }
        List<PendingJournalRecord> result = new ArrayList<>();
        for (Map.Entry<String, PendingJournalRecord> e : lastPendingById.entrySet()) {
            if (!committed.contains(e.getKey())) {
                result.add(e.getValue());
            }
        }
        return result;
    }
}
