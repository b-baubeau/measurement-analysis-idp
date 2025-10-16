#!/bin/python3
import pandas as pd

def stats(df: pd.DataFrame, time_frame: int, t_col: str = "timestamp", v_col: str = "latency") -> pd.DataFrame:
    """Compute basic statistics for latency and packet loss.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the data.
    time_frame : int
        Time frame in seconds to compute statistics over.
    t_col : str
        Name of the timestamp column.
    v_col : str
        Name of the column to compute statistics on (e.g., latency).

    Returns
    -------
    pd.DataFrame
        DataFrame containing the computed statistics.
    """
    df = df.sort_values(t_col).reset_index(drop=True)
    start_time = df[t_col].min()
    end_time = df[t_col].max()
    bins = pd.interval_range(start=start_time, end=end_time + time_frame, freq=time_frame, closed='left')
    df['time_bin'] = pd.cut(df[t_col], bins)

    stats_list = []
    for interval in bins:
        bin_data = df[df['time_bin'] == interval]
        if not bin_data.empty:
            stats_list.append({
                'time_bin_start': interval.left,
                'time_bin_end': interval.right,
                'mean': round(bin_data[v_col].mean(), 2),
                'median': round(bin_data[v_col].median(), 2),
                'min': bin_data[v_col].min(),
                'max': bin_data[v_col].max(),
                'std': round(bin_data[v_col].std(), 2),
                'count': bin_data[v_col].count()
            })

    stats_df = pd.DataFrame(stats_list)
    return stats_df

if __name__ == "__main__":
    df = pd.read_csv("merged_latency.csv")
    stats_df = stats(df, time_frame=10, t_col='timestamp', v_col='latency')
    print(stats_df)
    stats_df.to_csv("stats_latency.csv", index=False)

    df = pd.read_csv("merged_bw.csv")
    loss = (df['lost'] / df['total']) * 100
    stats_df = stats(df.assign(loss=loss), time_frame=10, t_col='timestamp', v_col='loss')
    print(stats_df)
    stats_df.to_csv("stats_loss.csv", index=False)