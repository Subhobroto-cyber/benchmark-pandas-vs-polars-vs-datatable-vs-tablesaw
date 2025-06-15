package org.example;
//32,906,496
import tech.tablesaw.api.Table;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.columns.Column;
import tech.tablesaw.io.csv.CsvWriteOptions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.*;
import java.util.function.Supplier;

import static tech.tablesaw.aggregate.AggregateFunctions.mean;

public class Benchmark1{

    private static final int RUNS = 5;
    private static final long MAX_ROWS = 30_300_000L;
    private static final String CSV_PATH =
            "C:/Users/Subhobroto Sasmal/Downloads/diabetes.csv";
    private static final DecimalFormat DF = new DecimalFormat("0.000");

    public static void main(String[] args) throws Exception {

        Map<String, List<Long>> timingsNs = new LinkedHashMap<>();
        Map<String, List<Long>> memDeltasMB = new LinkedHashMap<>();

        for (int i = 0; i < RUNS; i++) {

            /* ─────── READ CSV & TRIM TO MAX_ROWS ─────── */
            final Table table = measure("read", timingsNs, memDeltasMB, () -> {
                Table full = Table.read().csv(CSV_PATH);
                return full.rowCount() > MAX_ROWS
                        ? full.first((int) MAX_ROWS)
                        : full;
            });

            /* ─────── WRITE CSV ─────── */
            measure("write", timingsNs, memDeltasMB, () -> {
                try {
                    Path tmp = Files.createTempFile("tmp_", ".csv");
                    table.write().usingOptions(
                            CsvWriteOptions.builder(tmp.toFile()).build());
                    Files.deleteIfExists(tmp);
                } catch (Exception ex) {
                    System.err.println("tmp‑file cleanup failed: " + ex.getMessage());
                }
                return null;
            });

            /* ─────── GROUP BY Outcome, MEAN(Glucose) ─────── */
            measure("group", timingsNs, memDeltasMB,
                    () -> table.summarize("Glucose", mean).by("Outcome"));

            /* ─────── SORT BY Age DESC ─────── */
            measure("sort", timingsNs, memDeltasMB,
                    () -> table.sortDescendingOn("Age"));

            /* ─────── CONVERT TO 2D ARRAY ─────── */
            measure("to_np", timingsNs, memDeltasMB, () -> {
                int rows = table.rowCount();
                int cols = table.columnCount();
                double[][] arr = new double[rows][cols];

                for (int r = 0; r < rows; r++) {
                    for (int c = 0; c < cols; c++) {
                        Column<?> col = table.column(c);
                        ColumnType t = col.type();
                        if (t.equals(ColumnType.INTEGER) || t.equals(ColumnType.FLOAT) ||
                                t.equals(ColumnType.DOUBLE) || t.equals(ColumnType.LONG) ||
                                t.equals(ColumnType.SHORT)) {
                            Object v = col.get(r);
                            arr[r][c] = (v instanceof Number)
                                    ? ((Number) v).doubleValue()
                                    : Double.NaN;
                        } else {
                            arr[r][c] = Double.NaN;
                        }
                    }
                }
                return arr;
            });

            System.gc(); // Hint for GC before next iteration
        }

        /* ─────── SUMMARY ─────── */
        System.out.println("\n🏁  TABLESAW  median over " + RUNS + " runs");
        System.out.printf("%-8s %10s %10s%n", "stage", "sec", "ΔMB");

        timingsNs.keySet().forEach(stage -> {
            double medSec = median(timingsNs.get(stage)) / 1e9;
            double medMB = median(memDeltasMB.get(stage));
            System.out.printf("%-8s %10s %10.1f%n",
                    stage, DF.format(medSec), medMB);
        });
    }

    /* ─────── Utility Methods ─────── */

    private static <T> T measure(String label,
                                 Map<String, List<Long>> timesNs,
                                 Map<String, List<Long>> memDeltasMB,
                                 Supplier<T> block) {

        Runtime rt = Runtime.getRuntime();
        rt.gc(); // Best-effort clean slate
        long memBefore = usedMB(rt);

        long t0 = System.nanoTime();
        T result = block.get();
        long t1 = System.nanoTime();

        long deltaMB = Math.max(usedMB(rt) - memBefore, 0);

        timesNs.computeIfAbsent(label, k -> new ArrayList<>()).add(t1 - t0);
        memDeltasMB.computeIfAbsent(label, k -> new ArrayList<>()).add(deltaMB);

        return result;
    }

    private static long usedMB(Runtime rt) {
        return (rt.totalMemory() - rt.freeMemory()) >> 20; // Bytes to MB
    }

    private static double median(List<Long> vals) {
        Collections.sort(vals);
        return vals.get(vals.size() / 2);
    }
}
