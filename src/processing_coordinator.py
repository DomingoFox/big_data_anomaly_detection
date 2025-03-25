import pandas as pd
import time
from multiprocessing import Pool
from data_processor import DataProcessor
from pandas_spoofing_detector import PandasSpoofingDetector
from performance_evaluator import PerformanceEvaluator
from metrics_logger import log_metrics
from utils import time_tracker, log_aggregated_timings
from logger import Logger

class ProcessingCoordinator:
    def __init__(self, config, output_dir):
        self.config = config
        self.output_dir = output_dir
        self.logger = Logger(output_dir, verbose_timing=False)
        self.processor = DataProcessor(config.filepath)
        self.detector = PandasSpoofingDetector()
        self.evaluator = PerformanceEvaluator(output_dir)

    def process_batch(self, batch):
        # Process a single batch of data for parallel
        detector = PandasSpoofingDetector()
        return detector.detect_anomalies(batch)

    @time_tracker()
    def run_sequential(self):
        # Runs sequential
        self.logger.info("Starting sequential processing", category="status")
        start_time = time.time()
        df = self.processor.load_data_sequential()
        total_rows = len(df)
        anomalies = self.detector.detect_anomalies(df)
        processing_time = time.time() - start_time
        self.logger.info(f"Completed sequential processing in {processing_time:.2f} seconds", category="status")

        anomalies.to_csv(f"{self.output_dir}/detected_anomalies_sequential.csv", index=False)
        log_metrics(self.output_dir, "Sequential", None, total_rows, anomalies, processing_time)
        return processing_time

    @time_tracker()
    def run_parallel(self, batch_size, num_workers):
        # Runs parallel processing with given batch size and num of workers
        self.logger.info(f"Starting parallel processing with batch size {batch_size} and {num_workers} workers", category="status")
        start_time = time.time()
        total_rows = 0
        total_chunks = None
        try:
            test_reader = pd.read_csv(self.config.filepath, compression='zip', chunksize=batch_size)
            for chunk in test_reader:
                total_rows += len(chunk)
            total_chunks = total_rows // batch_size + (1 if total_rows % batch_size else 0)
        except Exception as e:
            self.logger.error(f"Failed to estimate total chunks: {str(e)}")

        chunks = self.processor.load_data_in_chunks(chunk_size=batch_size)
        with Pool(num_workers) as pool:
            self.logger.info(f"Processing {total_chunks if total_chunks else 'unknown'} chunks with {num_workers} workers", category="status")
            results = list(pool.imap_unordered(self.process_batch, chunks))
            log_aggregated_timings()
        anomalies = pd.concat(results)
        processing_time = time.time() - start_time
        self.logger.info(f"Completed parallel processing with batch size {batch_size} and {num_workers} workers in {processing_time:.2f} seconds", category="status")

        anomalies.to_csv(f"{self.output_dir}/detected_anomalies_parallel_batch_{batch_size}_workers_{num_workers}.csv", index=False)
        log_metrics(self.output_dir, "Parallel", batch_size, total_rows, anomalies, processing_time)
        return processing_time, total_rows