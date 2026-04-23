package io.jhtap.storage;

import java.nio.ByteBuffer;

public record Record(
    byte[] key,
    byte[][] fields,
    long timestamp,
    RecordType type
) {
    public enum RecordType {
        PUT((byte) 1),
        DELETE((byte) 0);

        private final byte code;

        RecordType(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return code;
        }

        public static RecordType fromCode(byte code) {
            return switch (code) {
                case 1 -> PUT;
                case 0 -> DELETE;
                default -> throw new IllegalArgumentException("Unknown RecordType code: " + code);
            };
        }
    }

    public int serializedSize() {
        // keyLen(4) + key + fieldsCount(4) + timestamp(8) + type(1)
        int size = 4 + key.length + 4 + 8 + 1;
        if (fields != null) {
            for (byte[] f : fields) {
                size += 4 + (f == null ? 0 : f.length);
            }
        }
        return size;
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putInt(key.length);
        buffer.put(key);
        if (fields == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(fields.length);
            for (byte[] f : fields) {
                if (f == null) {
                    buffer.putInt(-1);
                } else {
                    buffer.putInt(f.length);
                    buffer.put(f);
                }
            }
        }
        buffer.putLong(timestamp);
        buffer.put(type.getCode());
    }

    public static Record deserialize(ByteBuffer buffer) {
        int keyLength = buffer.getInt();
        if (keyLength < 0 || keyLength > buffer.remaining()) {
            throw new RuntimeException("Corrupted key length: " + keyLength);
        }
        byte[] key = new byte[keyLength];
        buffer.get(key);
        
        int fieldsCount = buffer.getInt();
        byte[][] fields = null;
        if (fieldsCount >= 0) {
            if (fieldsCount > 1000) throw new RuntimeException("Too many fields: " + fieldsCount);
            fields = new byte[fieldsCount][];
            for (int i = 0; i < fieldsCount; i++) {
                int fieldLen = buffer.getInt();
                if (fieldLen >= 0) {
                    if (fieldLen > buffer.remaining()) throw new RuntimeException("Corrupted field length");
                    fields[i] = new byte[fieldLen];
                    buffer.get(fields[i]);
                }
            }
        }
        long timestamp = buffer.getLong();
        RecordType type = RecordType.fromCode(buffer.get());
        return new Record(key, fields, timestamp, type);
    }

    public byte[] value() {
        return (fields != null && fields.length > 0) ? fields[0] : null;
    }
}
