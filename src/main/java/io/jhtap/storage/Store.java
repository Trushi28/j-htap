package io.jhtap.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Store implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Store.class);

    private final Path rootDir;
    private final AtomicLong nextTimestamp = new AtomicLong(System.currentTimeMillis());
    
    private MemTable memTable = new MemTable();
    private WriteAheadLog wal;
    private final List<StorageGroup> storageGroups = new CopyOnWriteArrayList<>();
    
    private final long memTableThreshold = 4 * 1024 * 1024; 
    private final CompactionThread compactionThread;

    public Store(Path rootDir) throws IOException {
        this.rootDir = rootDir;
        if (!Files.exists(rootDir)) {
            Files.createDirectories(rootDir);
        }
        this.wal = new WriteAheadLog(rootDir.resolve("wal.log"));
        recover();
        this.compactionThread = new CompactionThread();
        this.compactionThread.start();
    }

    private void recover() throws IOException {
        List<Record> records = wal.recover();
        for (Record r : records) {
            memTable.put(new InternalKey(r.key(), r.timestamp()), r);
            if (r.timestamp() >= nextTimestamp.get()) {
                nextTimestamp.set(r.timestamp() + 1);
            }
        }
        
        List<Path> sstFiles = Files.list(rootDir)
                .filter(p -> p.toString().endsWith(".sst"))
                .sorted()
                .collect(Collectors.toList());
        
        for (Path sst : sstFiles) {
            String base = sst.getFileName().toString().replace(".sst", "");
            Path col = rootDir.resolve(base + ".col");
            if (Files.exists(col)) {
                storageGroups.add(new StorageGroup(sst, col));
            }
        }
    }

    public synchronized void put(byte[] key, byte[][] fields) throws IOException {
        long ts = nextTimestamp.getAndIncrement();
        Record record = new Record(key, fields, ts, Record.RecordType.PUT);
        wal.append(record);
        memTable.put(new InternalKey(key, ts), record);

        if (memTable.getSize() >= memTableThreshold) {
            flush();
        }
    }

    public synchronized void put(byte[] key, byte[] value) throws IOException {
        put(key, new byte[][]{value});
    }

    public synchronized void delete(byte[] key) throws IOException {
        long ts = nextTimestamp.getAndIncrement();
        Record record = new Record(key, null, ts, Record.RecordType.DELETE);
        wal.append(record);
        memTable.put(new InternalKey(key, ts), record);

        if (memTable.getSize() >= memTableThreshold) {
            flush();
        }
    }

    public Record get(byte[] key, long snapshotTimestamp) throws IOException {
        Record r = memTable.get(key, snapshotTimestamp);
        if (r != null) return r;

        // Iterate backwards through storage groups
        // We use a regular list copy here because we need to acquire references safely
        List<StorageGroup> groupsSnapshot = new ArrayList<>(storageGroups);
        for (int i = groupsSnapshot.size() - 1; i >= 0; i--) {
            StorageGroup group = groupsSnapshot.get(i);
            if (group.acquire()) {
                try {
                    r = group.getSstReader().get(key, snapshotTimestamp);
                    if (r != null) return r;
                } finally {
                    group.release();
                }
            }
        }
        return null;
    }

    public long getCurrentTimestamp() {
        return nextTimestamp.get();
    }

    public List<StorageGroup> getStorageGroups() {
        return storageGroups;
    }

    public MemTable getMemTable() {
        return memTable;
    }

    public synchronized void flush() throws IOException {
        if (memTable.isEmpty()) return;

        String fileId = "table-" + System.currentTimeMillis();
        Path sstPath = rootDir.resolve(fileId + ".sst");
        Path colPath = rootDir.resolve(fileId + ".col");
        
        logger.info("Flushing MemTable to {} and {}", sstPath, colPath);
        
        new SSTableWriter(sstPath).write(memTable);
        new ColumnarWriter(colPath).write(memTable);

        storageGroups.add(new StorageGroup(sstPath, colPath));

        memTable.clear();
        wal.close();
        Files.delete(rootDir.resolve("wal.log"));
        wal = new WriteAheadLog(rootDir.resolve("wal.log"));
    }

    public List<Record> scan(long snapshotTimestamp) throws IOException {
        MemTable result = new MemTable();
        for (var entry : memTable.entries()) {
            InternalKey key = entry.getKey();
            if (key.timestamp() <= snapshotTimestamp && result.get(key.userKey(), snapshotTimestamp) == null) {
                result.put(key, entry.getValue());
            }
        }
        
        List<StorageGroup> groupsSnapshot = new ArrayList<>(storageGroups);
        for (int i = groupsSnapshot.size() - 1; i >= 0; i--) {
            StorageGroup group = groupsSnapshot.get(i);
            if (group.acquire()) {
                try {
                    var it = group.getSstReader().allRecordsIterator();
                    while (it.hasNext()) {
                        Record r = it.next();
                        if (r.timestamp() <= snapshotTimestamp && result.get(r.key(), snapshotTimestamp) == null) {
                            result.put(new InternalKey(r.key(), r.timestamp()), r);
                        }
                    }
                } finally {
                    group.release();
                }
            }
        }
        
        List<Record> records = new ArrayList<>();
        for (var entry : result.entries()) {
            if (entry.getValue().type() == Record.RecordType.PUT) {
                records.add(entry.getValue());
            }
        }
        return records;
    }

    @Override
    public synchronized void close() throws IOException {
        compactionThread.stopCompactor();
        wal.close();
        for (StorageGroup sg : storageGroups) {
            sg.markForDeletion(); // This will close and delete if refCount is 0
        }
    }

    private class CompactionThread extends Thread {
        private final AtomicBoolean running = new AtomicBoolean(true);

        public CompactionThread() {
            setDaemon(true);
            setName("Compaction-Daemon");
        }

        public void stopCompactor() {
            running.set(false);
            interrupt();
        }

        @Override
        public void run() {
            while (running.get()) {
                try {
                    Thread.sleep(1000); 
                    if (storageGroups.size() >= 4) {
                        logger.info("Triggering Background Compaction (Groups: {})", storageGroups.size());
                        
                        List<StorageGroup> toMerge = new ArrayList<>();
                        for (int i = 0; i < 3; i++) toMerge.add(storageGroups.get(i));
                        
                        Path target = rootDir.resolve("compacted-" + System.currentTimeMillis() + ".sst");

                        long gcThreshold = nextTimestamp.get() - 5000;
                        Compactor.compact(rootDir, toMerge, target, gcThreshold);
                        
                        StorageGroup merged = new StorageGroup(target, rootDir.resolve(target.getFileName().toString().replace(".sst", ".col")));
                        
                        synchronized (Store.this) {
                            storageGroups.removeAll(toMerge);
                            storageGroups.add(0, merged);
                        }

                        // Use our new Reference Counting for safe deletion
                        for (StorageGroup sg : toMerge) {
                            sg.markForDeletion();
                        }
                        logger.info("Compaction Complete. New Group: {}", merged.getSstPath().getFileName());
                    }
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    logger.error("Compaction failed", e);
                }
            }
        }
    }
}
