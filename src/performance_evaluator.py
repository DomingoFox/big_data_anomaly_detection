import pandas as pd
import matplotlib.pyplot as plt

class PerformanceEvaluator:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.metrics_file = f"{self.output_dir}/performance_metrics.csv"
        self.metrics = []

    def log_metrics(self, batch_size, num_workers, seq_time, par_time):
        metrics = {
            'batch_size': batch_size,
            'num_workers': num_workers,
            'sequential_time': seq_time,
            'parallel_time': par_time,
            'speedup': seq_time / par_time if par_time > 0 else 0
        }
        self.metrics.append(metrics)
        df = pd.DataFrame(self.metrics)
        df.to_csv(self.metrics_file, index=False)

    def visualize_metrics(self):
        df = pd.DataFrame(self.metrics)
        if df.empty:
            return

        # Plot speedup vs batch size for each num_workers
        for num_workers in df['num_workers'].unique():
            subset = df[df['num_workers'] == num_workers]
            plt.plot(subset['batch_size'], subset['speedup'], marker='o', label=f'{num_workers} workers')
        
        plt.xlabel('Batch Size')
        plt.ylabel('Speedup (Sequential/Parallel)')
        plt.title('Speedup vs Batch Size for Different Number of Workers')
        plt.legend()
        plt.grid(True)
        plt.savefig(f"{self.output_dir}/performance_plot.png")
        plt.close()