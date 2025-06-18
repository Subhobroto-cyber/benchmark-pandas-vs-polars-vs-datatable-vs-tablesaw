use polars::prelude::*;
use rand::Rng;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::{Duration, Instant};
use windows::{
    Win32::System::ProcessStatus::{K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS},
    Win32::System::Threading::GetCurrentProcess,
};

/// Returns the current process RAM usage in MB
fn get_ram_usage_mb() -> u64 {
    unsafe {
        let handle = GetCurrentProcess();
        let mut mem_counters = PROCESS_MEMORY_COUNTERS::default();
        if K32GetProcessMemoryInfo(
            handle,
            &mut mem_counters,
            std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        )
            .as_bool()
        {
            (mem_counters.WorkingSetSize / 1024 / 1024) as u64
        } else {
            0
        }
    }
}

/// Prints RAM usage after a given stage
fn print_ram(stage: &str) {
    let ram = get_ram_usage_mb();
    println!("📌 After {}: RAM Usage = {} MB", stage, ram);
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
    print_ram("CSV Generation");

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
    print_ram("CSV Read & Load");

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
    print_ram("Sort");

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
    print_ram("Filter");

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
    print_ram("GroupBy + Aggregate");

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
    println!("• RAM measurements show actual memory consumption patterns");
    println!("• Timing uses averages across multiple runs for accuracy");
    println!("• The lazy pipeline should be significantly faster due to optimizations");

    Ok(())
}