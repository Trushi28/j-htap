package io.jhtap.storage;

import java.util.Arrays;
import java.util.Comparator;

public record InternalKey(byte[] userKey, long timestamp) implements Comparable<InternalKey> {
    private static final Comparator<InternalKey> COMPARATOR = Comparator
        .comparing((InternalKey ik) -> ik.userKey, Arrays::compare)
        .thenComparingLong(InternalKey::timestamp).reversed(); // Descending for timestamp

    @Override
    public int compareTo(InternalKey other) {
        int res = Arrays.compare(this.userKey, other.userKey);
        if (res != 0) return res;
        // Same userKey, sort by timestamp descending (larger timestamp first)
        return Long.compare(other.timestamp, this.timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalKey that = (InternalKey) o;
        return timestamp == that.timestamp && Arrays.equals(userKey, that.userKey);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(userKey);
        result = 31 * result + Long.hashCode(timestamp);
        return result;
    }
}
