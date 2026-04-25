package io.jhtap.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class StoreTest {

    @Test
    public void testStoreFlushAndGet(@TempDir Path tempDir) throws IOException {
        try (Store store = new Store(tempDir)) {
            byte[] key = "key1".getBytes(StandardCharsets.UTF_8);
            byte[] val = "val1".getBytes(StandardCharsets.UTF_8);
            
            store.put(key, val);
            
            // Get before flush
            Record r1 = store.get(key, store.getCurrentTimestamp());
            assertNotNull(r1);
            assertArrayEquals(val, r1.value());
            
            store.flush();
            
            // Get after flush (from SSTable)
            Record r2 = store.get(key, store.getCurrentTimestamp());
            assertNotNull(r2);
            assertArrayEquals(val, r2.value());
        }
    }

    @Test
    public void testCompaction(@TempDir Path tempDir) throws IOException {
        try (Store store = new Store(tempDir)) {
            byte[] key1 = "a".getBytes(StandardCharsets.UTF_8);
            byte[] key2 = "b".getBytes(StandardCharsets.UTF_8);
            
            store.put(key1, ByteBuffer.allocate(8).putLong(10L).array());
            store.flush();
            
            store.put(key2, ByteBuffer.allocate(8).putLong(20L).array());
            store.flush();
            
            List<StorageGroup> groups = store.getStorageGroups();
            assertEquals(2, groups.size());
            
            Path target = tempDir.resolve("compacted.sst");
            // No GC in this test (threshold = 0)
            Compactor.compact(tempDir, groups, target, 0);
            
            assertTrue(Files.exists(target));
            Path colPath = tempDir.resolve("compacted.col");
            assertTrue(Files.exists(colPath));
            try (SSTableReader reader = new SSTableReader(target)) {
                Record r1 = reader.get(key1, store.getCurrentTimestamp());
                assertEquals(10L, ByteBuffer.wrap(r1.value()).getLong());
                
                Record r2 = reader.get(key2, store.getCurrentTimestamp());
                assertEquals(20L, ByteBuffer.wrap(r2.value()).getLong());
            }
            try (ColumnarReader reader = new ColumnarReader(colPath)) {
                assertEquals(30L, reader.sum(0));
            }
        }
    }
}
