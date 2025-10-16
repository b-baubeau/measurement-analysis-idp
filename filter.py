#!/bin/python3
import pandas as pd
from pathlib import Path
from collections.abc import Callable

DIR = Path("/media/marcel/TOSHIBA EXT/rosbags")

_ = lambda x: x
ns_to_s = lambda x: round(x * 1e-9) if isinstance(x, (int, float)) else x
round1 = lambda x: round(x, 1) if isinstance(x, float) else x
round3 = lambda x: round(x, 3) if isinstance(x, float) else x
round6 = lambda x: round(x, 6) if isinstance(x, float) else x

filters = {
    "bandwidth/fix": {"timestamp": ns_to_s, "latitude": round6, "longitude": round6},
    "bandwidth/uplink": {"timestamp": ns_to_s, "total": _, "lost": _, "time_interval": _},
    "vehicle/fix": {"timestamp": ns_to_s, "latitude": round6, "longitude": round6},
    "vehicle/network_metrics": {"timestamp": ns_to_s, "latency": round1, "tx_bitrate_mbps": round3}
}

def fuse_on_timestamp(df: pd.DataFrame, time_col: str = "timestamp") -> pd.DataFrame:
    """Only keep the first entry for each timestamp."""
    def fuse_group(group: pd.DataFrame, include_group: bool = True) -> pd.DataFrame:
        if include_group:
            return group.iloc[[0]]
        else:
            return group.iloc[0:1]

    return df.groupby(time_col).apply(fuse_group).reset_index(drop=True)

def filter_csv(path: Path, filter: dict[str, Callable]):
    df = pd.read_csv(path)
    df = df[list(filter.keys())]
    for col, func in filter.items():
        df[col] = df[col].apply(func)
    filtered = fuse_on_timestamp(df)
    # Filter zero values for lat/lon
    if 'latitude' in filtered.columns and 'longitude' in filtered.columns:
        filtered = filtered[(filtered['latitude'] != 0) & (filtered['longitude'] != 0)]
        filtered = filtered.dropna(subset=['latitude', 'longitude'])
    df = filtered.sort_values("timestamp").reset_index(drop=True)
    path_out = path.parent / f"filtered_{path.name}"
    df.to_csv(path_out, index=False)

if __name__ == "__main__":
    for key in filters.keys():
        path_in = DIR / f"{key}.csv"
        filter_csv(path_in, filters[key])