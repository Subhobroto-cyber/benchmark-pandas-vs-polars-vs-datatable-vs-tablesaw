import datatable as dt
import time
import os
import psutil
import numpy as np
import csv

def generate_test_data(n_rows, filename):
    """Generate test dataset with specified number of rows"""
    np.random.seed(42)
    dt.Frame({
        'id': range(n_rows),
        'category': np.random.choice([f'cat_{i}' for i in range(10)], size=n_rows),
        'value1': np.random.normal(100, 20, size=n_rows),
        'value2': np.random.uniform(0, 1000, size=n_rows)
    }).to_csv(filename)

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def benchmark_datatable(filename):
    """Run performance benchmark on datatable operations"""
    results = {}
    initial_memory = get_memory_usage()

    # Read operation
    start_time = time.time()
    df = dt.fread(filename)
    results['read'] = {
        'time': time.time() - start_time,
        'memory': get_memory_usage() - initial_memory
    }

    # Sort operation
    start_time = time.time()
    df_sorted = df.sort("value1")
    results['sort'] = {
        'time': time.time() - start_time,
        'memory': get_memory_usage() - initial_memory
    }

    # Filter operation
    start_time = time.time()
    df_filtered = df[dt.f.value1 > 110, :]
    results['filter'] = {
        'time': time.time() - start_time,
        'memory': get_memory_usage() - initial_memory
    }

    # GroupBy operation
    start_time = time.time()
    df_grouped = df[:, dt.mean(dt.f.value2), dt.by("category")]
    results['groupby'] = {
        'time': time.time() - start_time,
        'memory': get_memory_usage() - initial_memory
    }

    return results

def main():
    # Test with different dataset sizes
    sizes = [10000, 100000, 1000000]
    results_file = "datatable_results.csv"

    with open(results_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['size', 'operation', 'time', 'memory'])

        for size in sizes:
            filename = f"test_data_{size}.csv"
            generate_test_data(size, filename)
            results = benchmark_datatable(filename)

            for op, metrics in results.items():
                writer.writerow([size, op, metrics['time'], metrics['memory']])
                print(f"Size: {size}, Operation: {op}, Time: {metrics['time']:.4f}s, Memory: {metrics['memory']:.2f}MB")

if __name__ == "__main__":
    main()
