package io.jhtap.storage;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * Reads data from a columnar format (.col) using SIMD over memory-mapped files.
 */
public class ColumnarReader implements AutoCloseable {
    private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;
    
    private final FileChannel channel;
    private final MappedByteBuffer mappedData;
    private final int numRecords;
    private final int numCols;

    public ColumnarReader(Path path) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r");
        this.channel = raf.getChannel();
        long fileSize = channel.size();
        this.mappedData = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
        
        this.numRecords = mappedData.getInt(0);
        this.numCols = mappedData.getInt(4);
    }

    /**
     * Executes a vectorized sum over a specific column index.
     * Uses zero-copy MappedByteBuffer + Vector API.
     */
    public long sum(int colIdx) {
        if (colIdx >= numCols) return 0;

        // Column data starts at offset 8 + colIdx * numRecords * 8
        int startOffset = 8 + colIdx * numRecords * 8;
        
        long sum = 0;
        int i = 0;
        int loopBound = SPECIES.loopBound(numRecords);

        for (; i < loopBound; i += SPECIES.length()) {
            MemorySegment slice = MemorySegment.ofBuffer(mappedData).asSlice(startOffset + i * 8, (numRecords - i) * 8);
            LongVector v = LongVector.fromMemorySegment(SPECIES, slice, 0, mappedData.order());
            sum += v.reduceLanes(VectorOperators.ADD);
        }

        // Handle tails
        for (; i < numRecords; i++) {
            sum += mappedData.getLong(startOffset + i * 8);
        }
        
        return sum;
    }

    public long readLong(int colIdx, int rowIdx) {
        if (colIdx < 0 || colIdx >= numCols || rowIdx < 0 || rowIdx >= numRecords) {
            return 0L;
        }
        int startOffset = 8 + colIdx * numRecords * 8;
        return mappedData.getLong(startOffset + rowIdx * 8);
    }

    public int countGreaterThan(int colIdx, long threshold) {
        if (colIdx >= numCols) return 0;
        int startOffset = 8 + colIdx * numRecords * 8;
        
        int count = 0;
        int i = 0;
        int loopBound = SPECIES.loopBound(numRecords);

        for (; i < loopBound; i += SPECIES.length()) {
            MemorySegment slice = MemorySegment.ofBuffer(mappedData).asSlice(startOffset + i * 8, (numRecords - i) * 8);
            LongVector v = LongVector.fromMemorySegment(SPECIES, slice, 0, mappedData.order());
            count += (int) v.compare(VectorOperators.GT, threshold).trueCount();
        }

        for (; i < numRecords; i++) {
            if (mappedData.getLong(startOffset + i * 8) > threshold) count++;
        }
        
        return count;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
