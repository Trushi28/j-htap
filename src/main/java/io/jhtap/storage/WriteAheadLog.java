package io.jhtap.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class WriteAheadLog implements AutoCloseable {
    private final FileChannel channel;
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024); // 1MB write buffer

    public WriteAheadLog(Path path) throws IOException {
        this.channel = FileChannel.open(path, 
            StandardOpenOption.CREATE, 
            StandardOpenOption.WRITE,
            StandardOpenOption.READ);
        // Seek to end for appending
        channel.position(channel.size());
    }

    public synchronized void append(Record record) throws IOException {
        int size = record.serializedSize();
        if (buffer.remaining() < size) {
            flush();
        }
        record.serialize(buffer);
    }

    public synchronized void flush() throws IOException {
        if (buffer.position() == 0) return;
        buffer.flip();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        buffer.clear();
        channel.force(true);
    }

    public List<Record> recover() throws IOException {
        channel.position(0);
        List<Record> records = new ArrayList<>();
        ByteBuffer readBuffer = ByteBuffer.allocate(1024 * 1024);
        while (channel.read(readBuffer) != -1) {
            readBuffer.flip();
            while (readBuffer.remaining() >= 4) { // Minimum size to read key length
                int pos = readBuffer.position();
                try {
                    Record record = Record.deserialize(readBuffer);
                    records.add(record);
                } catch (Exception e) {
                    // Partial or corrupted record, stop here
                    readBuffer.position(pos);
                    break;
                }
            }
            readBuffer.compact();
        }
        return records;
    }

    @Override
    public void close() throws IOException {
        flush();
        channel.close();
    }
}
