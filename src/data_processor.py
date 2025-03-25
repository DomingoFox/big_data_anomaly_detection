
import pandas as pd
from utils import time_tracker

class DataProcessor:
    def __init__(self, filepath):
        self.filepath = filepath

    @time_tracker()
    def load_data_sequential(self):
        return pd.read_csv(self.filepath, compression='zip')

    @time_tracker()
    def load_data_in_chunks(self, chunk_size):
        return pd.read_csv(self.filepath, compression='zip', chunksize=chunk_size)