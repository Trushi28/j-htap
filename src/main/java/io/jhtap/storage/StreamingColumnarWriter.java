package io.jhtap.storage;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

final class StreamingColumnarWriter implements AutoCloseable {
    private final Path targetPath;
    private final List<Path> columnPaths = new ArrayList<>();
    private final List<DataOutputStream> columnStreams = new ArrayList<>();
    private int numCols = -1;
    private int numRecords;

    StreamingColumnarWriter(Path targetPath) {
        this.targetPath = targetPath;
    }

    void append(Record record) throws IOException {
        if (record.type() != Record.RecordType.PUT) {
            return;
        }
        byte[][] fields = record.fields();
        if (fields == null) {
            return;
        }
        ensureInitialized(fields.length);
        for (int colIdx = 0; colIdx < numCols; colIdx++) {
            byte[] field = colIdx < fields.length ? fields[colIdx] : null;
            if (field != null && field.length == 8) {
                columnStreams.get(colIdx).write(field);
            } else {
                columnStreams.get(colIdx).writeLong(0L);
            }
        }
        numRecords++;
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (DataOutputStream stream : columnStreams) {
            try {
                stream.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                }
            }
        }

        int safeNumCols = Math.max(numCols, 0);
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(
                targetPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        ))) {
            out.writeInt(numRecords);
            out.writeInt(safeNumCols);
            for (Path columnPath : columnPaths) {
                try (InputStream in = Files.newInputStream(columnPath)) {
                    in.transferTo(out);
                }
            }
        } finally {
            for (Path columnPath : columnPaths) {
                Files.deleteIfExists(columnPath);
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    private void ensureInitialized(int columns) throws IOException {
        if (numCols >= 0) {
            return;
        }
        numCols = columns;
        Path parent = targetPath.getParent() == null ? Path.of(".") : targetPath.getParent();
        String base = targetPath.getFileName().toString();
        for (int colIdx = 0; colIdx < numCols; colIdx++) {
            Path tempPath = Files.createTempFile(parent, base + ".col" + colIdx + ".", ".tmp");
            columnPaths.add(tempPath);
            columnStreams.add(new DataOutputStream(Files.newOutputStream(
                    tempPath,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING
            )));
        }
    }
}
