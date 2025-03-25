
import pandas as pd
import os

def ensure_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def log_metrics(output_dir, mode, batch_size, total_rows, combined_anomalies, processing_time):
    metrics_file = f"{output_dir}/metrics_summary.txt"
    total_anomalies = len(combined_anomalies)
    location_anomalies_count = len(combined_anomalies[combined_anomalies['anomaly_type'] == 'Location'])
    speed_anomalies_count = len(combined_anomalies[combined_anomalies['anomaly_type'] == 'Speed'])
    neighbour_anomalies_count = len(combined_anomalies[combined_anomalies['anomaly_type'] == 'Neighbour'])
    
    with open(metrics_file, 'a') as f:
        f.write(f"Mode: {mode}, Batch Size: {batch_size if batch_size else 'N/A'}\n")
        f.write("Library Used: Pandas\n")
        f.write(f"Total Rows Processed: {total_rows}\n")
        f.write(f"Total Processing Time: {processing_time:.2f} seconds\n")
        f.write(f"Total Anomalies Detected: {total_anomalies}\n")
        f.write(f"Location Anomalies: {location_anomalies_count}\n")
        f.write(f"Speed Anomalies: {speed_anomalies_count}\n")
        f.write(f"Neighbour Conflicts: {neighbour_anomalies_count}\n")
        f.write("---\n")