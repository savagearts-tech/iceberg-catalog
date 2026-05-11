package com.lakehouse.catalog.client.writer;

/**
 * Line state in {@link JsonLinePendingJournal}.
 */
public enum JournalState {
    PENDING,
    COMMITTED
}
