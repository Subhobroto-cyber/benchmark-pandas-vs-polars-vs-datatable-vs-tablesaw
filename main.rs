use polars::prelude::*;
use rand::Rng;
use std::fs::File;
use std::io::Write;
use std::time::Instant;
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

fn main() -> PolarsResult<()> {
    // 1. Generate CSV
    let mut rng = rand::thread_rng();
    let mut file = File::create("data.csv")?;
    writeln!(file, "id,category,value")?;
    for i in 0..100_000 {
        let category = format!("Category{}", rng.gen_range(1..=5));
        let value: f64 = rng.gen_range(0.0..1000.0);
        writeln!(file, "{},{},{}", i, category, value)?;
    }
    println!("✅ CSV generated.");
    print_ram("CSV Generation");

    // 2. Read CSV
    let start = Instant::now();
    let lf = LazyCsvReader::new("data.csv")
        .with_has_header(true)
        .finish()?;
    println!("✅ CSV read in: {:.3?}", start.elapsed());
    print_ram("CSV Read");

    // 3. Sort
    let start = Instant::now();
    let lf = lf.sort(["value"], Default::default());
    println!("✅ Sort: {:.3?}", start.elapsed());
    print_ram("Sort");

    // 4. Filter
    let start = Instant::now();
    let lf = lf.filter(col("value").gt(lit(500.0)));
    println!("✅ Filter: {:.3?}", start.elapsed());
    print_ram("Filter");

    // 5. GroupBy + Aggregate
    let start = Instant::now();
    let lf = lf
        .group_by([col("category")])
        .agg([
            col("id").mean().alias("id_mean"),
            col("value").mean().alias("value_mean"),
        ]);
    println!("✅ GroupBy: {:.3?}", start.elapsed());
    print_ram("GroupBy");

    // 6. Collect final result
    let df = lf.collect()?;
    println!("✅ Final DataFrame:\n{}", df);

    Ok(())
}
