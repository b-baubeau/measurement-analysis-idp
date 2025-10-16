#!/bin/bash
import pandas as pd
from pathlib import Path

DIR = Path("/media/marcel/TOSHIBA EXT/rosbags")
OUT_DIR = Path("./data")

def merge_on_positions(df1: pd.DataFrame, df2: pd.DataFrame, lat_col: str = "latitude", lon_col: str = "longitude", tolerance: float = 0.0001) -> pd.DataFrame:
    df1['key'] = 1
    df2['key'] = 1
    merged_df = pd.merge(df1, df2, on='key').drop('key', axis=1)
    condition = (abs(merged_df[lat_col + '_x'] - merged_df[lat_col + '_y']) <= tolerance) & \
                (abs(merged_df[lon_col + '_x'] - merged_df[lon_col + '_y']) <= tolerance)
    merged_df = merged_df[condition].dropna()
    merged_df.drop(columns=[lat_col + '_y', lon_col + '_y'], inplace=True)
    merged_df.rename(columns={lat_col + '_x': lat_col, lon_col + '_x': lon_col}, inplace=True)
    return merged_df

if __name__ == "__main__":
    bw_fix = pd.read_csv(DIR / "bandwidth/filtered_fix.csv")
    bw_uplink = pd.read_csv(DIR / "bandwidth/filtered_uplink.csv")

    vehicle_fix = pd.read_csv(DIR / "vehicle/filtered_fix.csv")
    vehicle_net = pd.read_csv(DIR / "vehicle/filtered_network_metrics.csv")

    # Bandwidth merging with its own fix data
    merged_bw = pd.merge_asof(bw_uplink.sort_values("timestamp"),
                            bw_fix.sort_values("timestamp"),
                            on="timestamp",
                            direction="nearest",
                            tolerance=1)  # 1 second tolerance
    merged_bw.to_csv(OUT_DIR / "merged_bw.csv", index=False)

    # Latency merging with vehicle fix data
    merged_latency = pd.merge_asof(vehicle_net.sort_values("timestamp"),
                                vehicle_fix.sort_values("timestamp"),
                                on="timestamp",
                                direction="nearest",
                                tolerance=1)  # 1 second tolerance
    merged_latency.to_csv(OUT_DIR / "merged_latency.csv", index=False)

    # Print max consecutive diff in latitude and longitude
    print("Max consecutive latitude diff (latency):", merged_latency['latitude'].diff().abs().max())
    print("Max consecutive longitude diff (latency):", merged_latency['longitude'].diff().abs().max())
    print("Max consecutive latitude diff (bandwidth):", merged_bw['latitude'].diff().abs().max())
    print("Max consecutive longitude diff (bandwidth):", merged_bw['longitude'].diff().abs().max())

    print("Mean latitude diff (latency):", merged_latency['latitude'].diff().abs().mean())
    print("Mean longitude diff (latency):", merged_latency['longitude'].diff().abs().mean())
    print("Mean latitude diff (bandwidth):", merged_bw['latitude'].diff().abs().mean())
    print("Mean longitude diff (bandwidth):", merged_bw['longitude'].diff().abs().mean())

    # Merge bandwidth and latency data on positions
    merged_data = merge_on_positions(merged_latency, merged_bw, tolerance=0.00015)
    merged_data.rename(columns={'timestamp_x': 'timestamp', 'timestamp_y': 'bw_timestamp'}, inplace=True)
    merged_data.to_csv(OUT_DIR / "merged_data.csv", index=False)