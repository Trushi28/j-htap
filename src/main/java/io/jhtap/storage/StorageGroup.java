package io.jhtap.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

public class StorageGroup implements AutoCloseable {
    private final SSTableReader sstReader;
    private final ColumnarReader colReader;
    private final Path sstPath;
    private final Path colPath;
    
    // Reference counter for concurrent readers vs background compactor
    private final AtomicInteger refCount = new AtomicInteger(1);
    private boolean markedForDeletion = false;

    public StorageGroup(Path sstPath, Path colPath) throws IOException {
        this.sstPath = sstPath;
        this.colPath = colPath;
        this.sstReader = new SSTableReader(sstPath);
        this.colReader = new ColumnarReader(colPath);
    }

    public SSTableReader getSstReader() { return sstReader; }
    public ColumnarReader getColReader() { return colReader; }
    public Path getSstPath() { return sstPath; }
    public Path getColPath() { return colPath; }

    public synchronized boolean acquire() {
        if (markedForDeletion) return false;
        refCount.incrementAndGet();
        return true;
    }

    public synchronized void release() {
        if (refCount.decrementAndGet() == 0 && markedForDeletion) {
            deleteFilesInternal();
        }
    }

    public synchronized void markForDeletion() {
        this.markedForDeletion = true;
        if (refCount.decrementAndGet() == 0) {
            deleteFilesInternal();
        }
    }

    private void deleteFilesInternal() {
        try {
            sstReader.close();
            colReader.close();
            Files.deleteIfExists(sstPath);
            Files.deleteIfExists(colPath);
        } catch (IOException e) {
            // Log or ignore
        }
    }

    @Override
    public void close() throws IOException {
        // Alias for markForDeletion to follow AutoCloseable
        markForDeletion();
    }
}
