package io.jhtap.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LSMTreeCoreTest {

    @Test
    public void testMemTablePutGet() {
        MemTable memTable = new MemTable();
        byte[] key = "key1".getBytes(StandardCharsets.UTF_8);
        byte[] val1 = "val1".getBytes(StandardCharsets.UTF_8);
        byte[] val2 = "val2".getBytes(StandardCharsets.UTF_8);

        memTable.put(new InternalKey(key, 100), new Record(key, new byte[][]{val1}, 100, Record.RecordType.PUT));
        memTable.put(new InternalKey(key, 200), new Record(key, new byte[][]{val2}, 200, Record.RecordType.PUT));

        // Get at timestamp 150 should see val1
        Record r1 = memTable.get(key, 150);
        assertNotNull(r1);
        assertArrayEquals(val1, r1.value());

        // Get at timestamp 250 should see val2
        Record r2 = memTable.get(key, 250);
        assertNotNull(r2);
        assertArrayEquals(val2, r2.value());

        // Get at timestamp 50 should see nothing
        Record r3 = memTable.get(key, 50);
        assertNull(r3);
    }

    @Test
    public void testWALRecovery(@TempDir Path tempDir) throws IOException {
        Path walPath = tempDir.resolve("test.wal");
        byte[] key = "key1".getBytes(StandardCharsets.UTF_8);
        byte[] val = "val1".getBytes(StandardCharsets.UTF_8);
        Record record = new Record(key, new byte[][]{val}, 123, Record.RecordType.PUT);

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.append(record);
            wal.flush();
        }

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            List<Record> recovered = wal.recover();
            assertEquals(1, recovered.size());
            Record r = recovered.get(0);
            assertArrayEquals(key, r.key());
            assertArrayEquals(val, r.value());
            assertEquals(123, r.timestamp());
        }
    }
}
