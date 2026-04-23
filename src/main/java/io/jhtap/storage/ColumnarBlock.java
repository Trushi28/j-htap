package io.jhtap.storage;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;

public class ColumnarBlock {
    private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;
    
    private final long[] data;
    private final int size;

    public ColumnarBlock(long[] data) {
        this.data = data;
        this.size = data.length;
    }

    /**
     * Example OLAP operation: Sum all values in the column using SIMD.
     */
    public long sum() {
        long sum = 0;
        int i = 0;
        int upperBound = SPECIES.loopBound(size);

        for (; i < upperBound; i += SPECIES.length()) {
            LongVector v = LongVector.fromArray(SPECIES, data, i);
            sum += v.reduceLanes(jdk.incubator.vector.VectorOperators.ADD);
        }

        for (; i < size; i++) {
            sum += data[i];
        }
        return sum;
    }

    /**
     * Example OLAP operation: Filter values > threshold.
     */
    public int countGreaterThan(long threshold) {
        int count = 0;
        int i = 0;
        int upperBound = SPECIES.loopBound(size);

        for (; i < upperBound; i += SPECIES.length()) {
            LongVector v = LongVector.fromArray(SPECIES, data, i);
            count += (int) v.compare(jdk.incubator.vector.VectorOperators.GT, threshold).trueCount();
        }

        for (; i < size; i++) {
            if (data[i] > threshold) count++;
        }
        return count;
    }
}
