
from config import ProcessingConfig
from metrics_logger import ensure_directory
from processing_coordinator import ProcessingCoordinator

def main():

    # Setup config
    config = ProcessingConfig()
    ensure_directory(config.output_dir)
    coordinator = ProcessingCoordinator(config, config.output_dir)

    # Sequential
    seq_time = coordinator.run_sequential()

    # Parallel for each batch size and cpu count
    for num_workers in config.cpu_choices:
        for batch_size in config.batch_sizes:
            par_time, total_rows = coordinator.run_parallel(batch_size, num_workers)
            coordinator.evaluator.log_metrics(batch_size, num_workers, seq_time, par_time)

    # Visualizations
    coordinator.evaluator.visualize_metrics()

if __name__ == "__main__":
    main()