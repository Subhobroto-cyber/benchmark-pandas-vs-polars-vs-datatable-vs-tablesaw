package org.example;

import tech.tablesaw.api.Table;
import tech.tablesaw.aggregate.AggregateFunctions;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class TablesawBenchmark1{

    static class BenchmarkResult {
        long timeMs;
        long memoryMb;
        BenchmarkResult(long timeMs, long memoryMb) {
            this.timeMs = timeMs;
            this.memoryMb = memoryMb;
        }
    }

    private static long getMemoryUsage() {
        Runtime rt = Runtime.getRuntime();
        return (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
    }

    private static void generateTestData(int nRows, String file) throws IOException {
        Random rand = new Random(42);
        try (PrintWriter out = new PrintWriter(new FileWriter(file))) {
            out.println("id,category,value1,value2");
            for (int i = 0; i < nRows; i++) {
                String category = "cat_" + rand.nextInt(10);
                double value1 = rand.nextGaussian() * 20 + 100;
                double value2 = rand.nextDouble() * 1000;
                out.printf("%d,%s,%.2f,%.2f%n", i, category, value1, value2);
            }
        }
    }

    private static Map<String, BenchmarkResult> benchmarkTablesaw(String file) throws IOException {
        Map<String, BenchmarkResult> res = new HashMap<>();
        long base = getMemoryUsage();

        long t = System.currentTimeMillis();
        Table tbl = Table.read().csv(file);
        res.put("read",    new BenchmarkResult(System.currentTimeMillis() - t, getMemoryUsage() - base));

        t = System.currentTimeMillis();
        tbl.sortOn("value1");
        res.put("sort",    new BenchmarkResult(System.currentTimeMillis() - t, getMemoryUsage() - base));

        t = System.currentTimeMillis();
        tbl.where(tbl.doubleColumn("value1").isGreaterThan(110));
        res.put("filter",  new BenchmarkResult(System.currentTimeMillis() - t, getMemoryUsage() - base));

        t = System.currentTimeMillis();
        tbl.summarize("value2", AggregateFunctions.mean).by("category");
        res.put("groupby", new BenchmarkResult(System.currentTimeMillis() - t, getMemoryUsage() - base));

        return res;
    }

    public static void main(String[] args) throws IOException {

        /* JVM warm‑up (ignore these results) */
        generateTestData(1_000, "warmup_data.csv");
        benchmarkTablesaw("warmup_data.csv");

        int[] sizes = {10_000, 100_000, 1_000_000};

        try (PrintWriter out = new PrintWriter(new FileWriter("tablesaw_results.csv"))) {
            out.println("size,operation,time_ms,memory_mb");

            for (int size : sizes) {
                String csv = "test_data_" + size + ".csv";
                generateTestData(size, csv);
                Map<String, BenchmarkResult> results = benchmarkTablesaw(csv);

                for (Map.Entry<String, BenchmarkResult> e : results.entrySet()) {
                    out.printf("%d,%s,%d,%d%n",
                            size, e.getKey(), e.getValue().timeMs, e.getValue().memoryMb);
                    System.out.printf("Size: %d  Operation: %-7s  Time: %d ms  Memory: %d MB%n",
                            size, e.getKey(), e.getValue().timeMs, e.getValue().memoryMb);
                }
            }
        }
    }
}
