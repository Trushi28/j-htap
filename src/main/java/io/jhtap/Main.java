package io.jhtap;

import io.jhtap.storage.QueryEngine;
import io.jhtap.storage.Record;
import io.jhtap.storage.Store;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        System.out.println("=== J-HTAP Engine Console ===");
        System.out.println("Commands: SELECT, INSERT, SUM. Type 'exit' to quit.");
        
        try {
            Path root = Paths.get("data");
            Store store = new Store(root);
            QueryEngine engine = new QueryEngine(store);
            Scanner scanner = new Scanner(System.in);

            while (true) {
                System.out.print("j-htap> ");
                String line = scanner.nextLine().trim();
                if (line.equalsIgnoreCase("exit")) break;
                if (line.isEmpty()) continue;

                try {
                    Object result = engine.execute(line);
                    printResult(result);
                } catch (Exception e) {
                    System.err.println("Error: " + e.getMessage());
                }
            }
            store.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printResult(Object result) {
        if (result == null) {
            System.out.println("Empty result.");
        } else if (result instanceof Record r) {
            System.out.println(formatRecord(r));
        } else if (result instanceof List<?> list) {
            System.out.println("Results: " + list.size());
            for (Object o : list) {
                if (o instanceof Record r) System.out.println("  " + formatRecord(r));
            }
        } else {
            System.out.println(result);
        }
    }

    private static String formatRecord(Record r) {
        StringBuilder sb = new StringBuilder();
        sb.append("Key: ").append(new String(r.key())).append(" [TS: ").append(r.timestamp()).append("] ");
        if (r.fields() != null) {
            sb.append("Fields: ");
            for (byte[] f : r.fields()) {
                if (f.length == 8) sb.append(java.nio.ByteBuffer.wrap(f).getLong()).append(" ");
                else sb.append(new String(f)).append(" ");
            }
        }
        return sb.toString();
    }
}
