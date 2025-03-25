
class ProcessingConfig:

    def __init__(self):
        self.date = "2024-08-28"
        self.filepath = f"files\\aisdk-{self.date}.zip"
        self.output_dir = "results"
        self.batch_sizes = [100000, 500000]
        self.cpu_choices = [4, 8]

        # For smaller size testing if needed:
        # self.filepath = f"files\\aisdk-{self.date}\\aisdk-{self.date}_small.zip"