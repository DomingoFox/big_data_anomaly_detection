import pandas as pd
import numpy as np
from utils import time_tracker

class PandasSpoofingDetector:
    def __init__(self):
        self.speed_threshold = 50  # knots
        self.distance_threshold = 100  # km
        self.time_window = '1min'  # 1 min time window for neighbours
        self.neighbour_threshold = 100 # Reports threshold
        self.unique_mmsi_threshold = 50  # Minimum unique MMSI for a conflict

    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """Calculate Haversine distance between two points in kilometers."""
        R = 6371  # Earths radius in km
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        return R * c

    @time_tracker()
    def detect_location_anomalies(self, df):
        """Detect sudden location jumps."""
        df = df.sort_values(['MMSI', '# Timestamp'])
        df['prev_lat'] = df.groupby('MMSI')['Latitude'].shift(1)
        df['prev_lon'] = df.groupby('MMSI')['Longitude'].shift(1)
        df['distance'] = self.calculate_distance(df['Latitude'], df['Longitude'], df['prev_lat'], df['prev_lon'])
        location_anomalies = df[df['distance'] > self.distance_threshold].copy()
        location_anomalies['anomaly_type'] = 'Location'
        return location_anomalies

    @time_tracker()
    def detect_speed_anomalies(self, df):
        """Detect inconsistent speed changes."""
        df['speed_change'] = df.groupby('MMSI')['SOG'].diff().abs()
        speed_anomalies = df[df['speed_change'] > self.speed_threshold].copy()
        speed_anomalies['anomaly_type'] = 'Speed'
        return speed_anomalies

    @time_tracker()
    def detect_neighbour_conflicts(self, df):
        """Identify vessels in same region with conflicting positions within a time window."""
        # Ensure timestamp is in datetime format with correct parsing
        df['# Timestamp'] = pd.to_datetime(df['# Timestamp'], dayfirst=True, errors='coerce')
        
        # Round latitude and longitude for spatial grouping
        df['rounded_lat'] = df['Latitude'].round(3)
        df['rounded_lon'] = df['Longitude'].round(3)
        
        # Group by location and time window (1 minute)
        df['time_bucket'] = df['# Timestamp'].dt.floor(self.time_window)
        
        # Calculate group size and unique MMSI count
        grouped = df.groupby(['rounded_lat', 'rounded_lon', 'time_bucket']).agg(
            count=('MMSI', 'size'),
            unique_mmsi=('MMSI', 'nunique')
        )
        
        # Identify conflict areas: more than 100 reports and 50 unique MMSI
        conflict_areas = grouped[
            (grouped['count'] > self.neighbour_threshold) & 
            (grouped['unique_mmsi'] > self.unique_mmsi_threshold)
        ].index
        
        # Filter rows that match these conflict areas
        neighbour_conflicts = df[
            df[['rounded_lat', 'rounded_lon', 'time_bucket']].apply(tuple, axis=1).isin(conflict_areas)
        ].copy()
        neighbour_conflicts['anomaly_type'] = 'Neighbour'
        return neighbour_conflicts

    @time_tracker()
    def detect_anomalies(self, df):
        """Detect anomalies and flag their type without dropping duplicates."""
        location_anomalies = self.detect_location_anomalies(df)
        speed_anomalies = self.detect_speed_anomalies(df)
        neighbour_conflicts = self.detect_neighbour_conflicts(df)
        combined_anomalies = pd.concat([location_anomalies, speed_anomalies, neighbour_conflicts])
        return combined_anomalies