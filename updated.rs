use polars::prelude::*;
use rand::Rng;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::{Duration, Instant};
use windows::{
    Win32::System::ProcessStatus::{K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS_EX},
    Win32::System::Threading::GetCurrentProcess,
};

#[derive(Debug)]
struct MemoryMetrics {
    working_set_mb: u64,
    private_usage_mb: u64,
    pagefile_usage_mb: u64,
    peak_working_set_mb: u64,
}

/// Returns comprehensive memory metrics for the current process
fn get_memory_metrics() -> MemoryMetrics {
    unsafe {
        let handle = GetCurrentProcess();
        let mut mem_counters = PROCESS_MEMORY_COUNTERS_EX::default();

        if K32GetProcessMemoryInfo(
            handle,
            std::ptr::addr_of_mut!(mem_counters) as *mut _ as *mut _,
            std::mem::size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32,
        )
            .as_bool()
        {
            MemoryMetrics {
                working_set_mb: (mem_counters.WorkingSetSize / 1024 / 1024) as u64,
                private_usage_mb: (mem_counters.PrivateUsage / 1024 / 1024) as u64,
                pagefile_usage_mb: (mem_counters.PagefileUsage / 1024 / 1024) as u64,
                peak_working_set_mb: (mem_counters.PeakWorkingSetSize / 1024 / 1024) as u64,
            }
        } else {
            MemoryMetrics {
                working_set_mb: 0,
                private_usage_mb: 0,
                pagefile_usage_mb: 0,
                peak_working_set_mb: 0,
            }
        }
    }
}

/// Prints comprehensive memory usage after a given stage
fn print_memory_detailed(stage: &str) {
    let metrics = get_memory_metrics();
    println!("📌 After {}: ", stage);
    println!("   • Working Set: {} MB (physical RAM currently used)", metrics.working_set_mb);
    println!("   • Private Usage: {} MB (actual process allocation)", metrics.private_usage_mb);
    println!("   • Pagefile Usage: {} MB (virtual memory used)", metrics.pagefile_usage_mb);
    println!("   • Peak Working Set: {} MB (highest physical RAM usage)", metrics.peak_working_set_mb);
}

/// Simple RAM usage for quick monitoring (backwards compatibility)
fn print_ram(stage: &str) {
    let metrics = get_memory_metrics();
    println!("📌 After {}: RAM = {} MB (Working Set), Private = {} MB",
             stage, metrics.working_set_mb, metrics.private_usage_mb);
}

/// Times an operation multiple times and returns average duration
fn time_operation<F, T>(operation: F, trials: usize, name: &str) -> (T, Duration)
where
    F: Fn() -> PolarsResult<T>,
{
    let mut durations = Vec::new();
    let mut result = None;

    for _ in 0..trials {
        let start = Instant::now();
        let op_result = operation().expect("Operation failed");
        durations.push(start.elapsed());
        result = Some(op_result);
    }

    let avg_duration = durations.iter().sum::<Duration>() / trials as u32;
    println!("✅ {} completed in (avg of {} runs): {:.3?}", name, trials, avg_duration);

    (result.unwrap(), avg_duration)
}

fn main() -> PolarsResult<()> {
    println!("🚀 Starting Polars Performance Benchmark\n");

    // 1. Generate CSV with buffered writing
    println!("📝 Generating CSV data...");
    let start = Instant::now();
    let mut rng = rand::thread_rng();
    let file = File::create("data.csv").expect("Failed to create CSV file");
    let mut writer = BufWriter::new(file);

    writeln!(writer, "id,category,value").expect("Failed to write header");
    for i in 0..100_000 {
        writeln!(
            writer,
            "{},{},{}",
            i,
            format!("Category{}", rng.gen_range(1..=5)),
            rng.gen_range(0.0..1000.0)
        ).expect("Failed to write data row");
    }
    drop(writer); // Ensure buffer is flushed

    println!("✅ CSV generated in: {:.3?}", start.elapsed());
    print_memory_detailed("CSV Generation");

    println!("\n--- Testing Individual Operations (Forced Execution) ---");

    // 2. Read CSV and force execution
    let (mut df, _) = time_operation(
        || {
            LazyCsvReader::new("data.csv")
                .with_has_header(true)
                .finish()?
                .collect()
        },
        3,
        "CSV Read & Load"
    );
    print_memory_detailed("CSV Read & Load");

    // 3. Sort (force execution with multiple trials)
    let (sorted_df, _) = time_operation(
        || {
            df.clone().lazy()
                .sort(["value"], Default::default())
                .collect()
        },
        3,
        "Sort"
    );
    df = sorted_df;
    print_memory_detailed("Sort");

    // 4. Filter (force execution with multiple trials)
    let (filtered_df, _) = time_operation(
        || {
            df.clone().lazy()
                .filter(col("value").gt(lit(500.0)))
                .collect()
        },
        3,
        "Filter"
    );
    df = filtered_df;
    print_memory_detailed("Filter");

    // 5. GroupBy + Aggregate (force execution with multiple trials)
    let (grouped_df, _) = time_operation(
        || {
            df.clone().lazy()
                .group_by([col("category")])
                .agg([
                    col("id").mean().alias("id_mean"),
                    col("value").mean().alias("value_mean"),
                ])
                .collect()
        },
        3,
        "GroupBy + Aggregate"
    );
    df = grouped_df;
    print_memory_detailed("GroupBy + Aggregate");

    println!("\n--- Testing Optimized Lazy Pipeline ---");

    // Full lazy pipeline (the proper way)
    let lazy_pipeline = LazyCsvReader::new("data.csv")
        .with_has_header(true)
        .finish()?
        .sort(["value"], Default::default())
        .filter(col("value").gt(lit(500.0)))
        .group_by([col("category")])
        .agg([
            col("id").mean().alias("id_mean"),
            col("value").mean().alias("value_mean"),
        ]);

    // Show the optimized plan
    println!("\n🧠 Optimized Query Plan:");
    println!("{}", lazy_pipeline.describe_optimized_plan()?);

    // Time the full lazy execution
    let (lazy_result, _) = time_operation(
        || lazy_pipeline.clone().collect(),
        5,
        "Full Lazy Pipeline"
    );
    print_ram("Full Lazy Pipeline");

    println!("\n📊 Final Results:");
    println!("Individual operations result:\n{}", df);
    println!("\nLazy pipeline result:\n{}", lazy_result);

    println!("\n📋 SUMMARY:");
    println!("• Individual operations: Each step forced to execute separately");
    println!("• Lazy pipeline: All operations optimized and executed together");
    println!("• Memory metrics explained:");
    println!("  - Working Set: Physical RAM currently used by process");
    println!("  - Private Usage: Actual memory allocated to process (most accurate)");
    println!("  - Pagefile Usage: Virtual memory used (includes swapped memory)");
    println!("  - Peak Working Set: Highest physical RAM usage during execution");
    println!("• Timing uses averages across multiple runs for accuracy");
    println!("• The lazy pipeline should be significantly faster due to optimizations");
    println!("\n💡 External Profiling Options:");
    println!("• Process Explorer: Real-time memory monitoring");
    println!("• Windows Performance Monitor: Detailed system metrics");
    println!("• For Linux: heaptrack, Valgrind massif");

    Ok(())
}