package io.jhtap.storage;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Map;

/**
 * Writes data in a columnar format (.col) optimized for SIMD scans.
 * The layout is: [num_records][num_cols]
 * followed by data for col 0 (num_records * field_size)
 * followed by data for col 1 (num_records * field_size)
 * ...
 * For this implementation, we assume all fields in a column have the same size (e.g. 8 bytes for long).
 */
public class ColumnarWriter {
    private final Path path;

    public ColumnarWriter(Path path) {
        this.path = path;
    }

    public void write(MemTable memTable) throws IOException {
        if (memTable.isEmpty()) return;

        // Determine number of columns from the first record
        var firstEntry = memTable.entries().iterator().next();
        byte[][] fields = firstEntry.getValue().fields();
        if (fields == null) return;
        int numCols = fields.length;
        
        // Count records (this is a flush, so we know the size from the memtable map)
        int numRecords = 0;
        for (var entry : memTable.entries()) {
            if (entry.getValue().type() == Record.RecordType.PUT) {
                numRecords++;
            }
        }

        try (FileOutputStream fos = new FileOutputStream(path.toFile());
             DataOutputStream out = new DataOutputStream(fos)) {
            
            out.writeInt(numRecords);
            out.writeInt(numCols);

            // Write each column's data contiguously
            for (int colIdx = 0; colIdx < numCols; colIdx++) {
                for (var entry : memTable.entries()) {
                    Record r = entry.getValue();
                    if (r.type() == Record.RecordType.PUT) {
                        byte[] field = (r.fields() != null && colIdx < r.fields().length) ? r.fields()[colIdx] : null;
                        if (field != null && field.length == 8) {
                            out.write(field);
                        } else {
                            // Pad or handle nulls - for simplicity in SIMD, we'll write 0L
                            out.writeLong(0L);
                        }
                    }
                }
            }
            out.flush();
        }
    }
}
