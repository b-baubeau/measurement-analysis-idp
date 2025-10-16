#!/bin/python3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.figure import Figure
from pathlib import Path

DIR = Path("./data")
OUT_DIR = Path("./plots")

def plot_network_metrics(t: pd.Series|dict[str,pd.Series], latency: pd.Series, loss: pd.Series):
    """Plot network metrics over time.

    Parameters
    ----------
    t : pd.Series or dict of pd.Series
        Time series in seconds. If dict, should contain 'latency' and 'loss' keys.
    latency : pd.Series
        Latency values in milliseconds.
    loss : pd.Series
        Packet loss values as a proportion (0 to 1)."""
    fig, ax1 = plt.subplots(figsize=(12, 4))

    s = 3
    color = 'tab:blue'
    ax1.set_xlabel('Time (s)')
    ax1.set_ylabel('Latency (ms)', color=color)
    if isinstance(t, dict):
        ax1.scatter(t['latency'], latency, color=color, s=s)
    else:
        ax1.scatter(t, latency, color=color, s=s)
    ax1.set_ylim(0, latency.max() * 1.1)
    ax1.tick_params(axis='y', labelcolor=color)

    ax2 = ax1.twinx()  
    color = 'tab:red'
    ax2.set_ylabel('Bandwidth over time (Mbps)', color=color)
    if isinstance(t, dict):
        ax2.scatter(t['loss'], 50 * (1 - loss), color=color, s=s)
    else:
        ax2.scatter(t, 50 * (1 - loss), color=color, s=s)
    ax2.set_ylim(0, 55)
    ax2.tick_params(axis='y', labelcolor=color)

    fig.tight_layout()  
    # plt.show(block=False)
    return fig

def plot_bandwidth(t: pd.Series, bw: pd.Series, tx_bitrate: pd.Series):
    """Plot bandwidth over time along with transmitted bitrate.

    Parameters
    ----------
    t : pd.Series
        Time series in seconds.
    bw : pd.Series
        Bandwidth values in Mbps.
    tx_bitrate : pd.Series
        Transmit bitrate values in Mbps.
    rx_bitrate : pd.Series
        Receive bitrate values in Mbps."""
    fig = plt.figure(figsize=(12, 4))

    s = 3
    color = 'tab:red'
    plt.scatter(t, bw, color=color, s=s, label='Estimated bandwidth (Mbps)')
    plt.plot(t, tx_bitrate, color='tab:orange', label='Transmit bitrate (Mbps)', linewidth=0.5)
    plt.xlabel('Time (s)')
    plt.ylabel('Estimated bandwidth (Mbps)', color=color)
    plt.tick_params(axis='y', labelcolor=color)
    plt.legend(loc='lower right')
    plt.ylim(-1, 55)
    plt.grid(True)

    fig.tight_layout()  
    # plt.show(block=False)
    return fig

def plot_latency_distribution(latency: pd.Series):
    """Plot latency distribution as a histogram.

    Parameters
    ----------
    latency : pd.Series
        Latency values in milliseconds."""
    fig = plt.figure(figsize=(8, 4))
    plt.hist(latency, bins=100, color='tab:blue', alpha=0.7)
    plt.xlabel('Latency (ms)')
    plt.ylabel('Frequency')
    plt.title('Latency Distribution')
    plt.grid(True)
    plt.tight_layout()
    #plt.show()

    return fig

def plot_gps(lat: pd.Series, lon: pd.Series, z: pd.Series,
             label: str, cmap: mcolors.Colormap) -> Figure:
    """Plot GPS trajectory.

    Parameters
    ----------
    lat : pd.Series
        Latitude values.
    lon : pd.Series
        Longitude values."""
    fig = plt.figure()
    plt.scatter(lon, lat, c=z, cmap=cmap, s=1)
    cbar = plt.colorbar()
    cbar.set_label(label)
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.axis('equal')
    plt.grid(True)
    plt.tight_layout()
    #plt.show()

    return fig

def plot_frame_drops(t, is_freezed, threshold: int = 2):
    """Plot frame freeze events over time.

    Parameters
    ----------
    t : pd.Series
        Time series in seconds.
    is_freezed : pd.Series
        Binary series indicating if the frame is freezed (1) or not (0).
    threshold : float, optional
        Threshold in consecutive frames to consider a frame as freezed, by default 2."""
    fig, ax1 = plt.subplots(figsize=(12, 4))
    i = 0
    while i < len(is_freezed) - threshold:
        if all(is_freezed[i:i+threshold]):
            i += threshold
        else:
            is_freezed[i] = 0
            i += 1

    color = 'tab:blue'
    ax1.set_xlabel('Time (s)')
    ax1.set_ylabel('Frame freezed', color=color)
    ax1.plot(t, is_freezed, color=color, drawstyle='steps-post')
    ax1.set_ylim(0, 1.05)
    ax1.tick_params(axis='y', labelcolor=color)

    # proportion of freezed frames over last second
    window_size = int(40)
    freezed_proportion = is_freezed.rolling(window=window_size, min_periods=1).mean()
    ax2 = ax1.twinx()
    color = 'tab:red'
    ax2.set_ylabel('Proportion of freezed frames (last second)', color=color)
    ax2.plot(t, freezed_proportion, color=color)
    ax2.set_ylim(0, 1.05)
    ax2.tick_params(axis='y', labelcolor=color)

    fig.tight_layout()
    #plt.show()

    return fig


if __name__ == "__main__":
    bw_df = pd.read_csv(DIR / "merged_bw.csv")
    latency_df = pd.read_csv(DIR / "merged_latency.csv")
    merged_df = pd.read_csv(DIR / "merged_data.csv")

    # Loss and bandwidth maps
    lat = bw_df['latitude']
    lon = bw_df['longitude']
    loss = bw_df['lost'] / bw_df['total'] * 100
    bw = 50 * (1 - loss/100) # convert to bandwidth in Mbps
    cdict = {
        'red':   ((0.0, 0, 0),
                  (0.05, 0, 0),
                  (0.1, 0.8, 0.8),
                  (1.0, 0.8, 0.8)),
        'green': ((0.0, 0.8, 0.8),
                  (0.1, 0.8, 0.8),
                  (0.6, 0, 0),
                  (1.0, 0, 0)),
        'blue':  ((0.0, 0, 0),
                  (1.0, 0, 0))
    }
    cmap = mcolors.LinearSegmentedColormap('RedGreen', cdict)

    fig = plot_gps(lat, lon, loss, label="Packet loss (%)", cmap=cmap)
    fig.savefig(OUT_DIR / "loss_map.pdf", format='pdf', dpi=300)

    cdict2 = {
        'red':   ((0.0, 0.8, 0.8),
                  (0.9, 0.8, 0.8),
                  (0.95, 0, 0),
                  (1.0, 0, 0)),
        'green': ((0.0, 0, 0),
                  (0.4, 0, 0),
                  (0.9, 0.8, 0.8),
                  (1.0, 0.8, 0.8)),
        'blue':  ((0.0, 0, 0),
                  (1.0, 0, 0))
    }
    cmap2 = mcolors.LinearSegmentedColormap('GreenRed', cdict2)
    fig = plot_gps(lat, lon, bw, label="Estimated bandwidth (Mbps)", cmap=cmap2)
    fig.savefig(OUT_DIR / "bandwidth_map.pdf", format='pdf', dpi=300)

    # Latency map
    lat = latency_df['latitude']
    lon = latency_df['longitude']
    latency = latency_df['latency']

    fig = plot_gps(lat, lon, latency, label="Latency (ms)", cmap=cmap)
    fig.savefig(OUT_DIR / "latency_map.pdf", format='pdf', dpi=300)

    # Merged loss and latency maps
    lat = merged_df['latitude']
    lon = merged_df['longitude']
    z1 = merged_df['latency']
    z2 = merged_df['lost'] / merged_df['total'] * 100
    z3 = 50 * (1 - z2/100) # convert to bandwidth in Mbps

    fig = plot_gps(lat, lon, z1, label="Latency (ms)", cmap=cmap)
    fig.savefig(OUT_DIR / "merged_latency_map.pdf", format='pdf', dpi=300)
    fig = plot_gps(lat, lon, z2, label="Packet loss (%)", cmap=cmap)
    fig.savefig(OUT_DIR / "merged_loss_map.pdf", format='pdf', dpi=300)
    fig = plot_gps(lat, lon, z3, label="Estimated bandwidth (Mbps)", cmap=cmap2)
    fig.savefig(OUT_DIR / "merged_bandwidth_map.pdf", format='pdf', dpi=300)

    # Network metrics over time
    t = {'latency': latency_df['timestamp'] - latency_df['timestamp'].min(),
         'loss': merged_df['timestamp'] - merged_df['timestamp'].min()}
    latency = latency_df['latency']
    loss = merged_df['lost'] / merged_df['total']

    fig = plot_network_metrics(t, latency, loss)
    fig.savefig(OUT_DIR / "network_metrics_time.pdf", format='pdf', dpi=300)

    # Bandwidth over time
    t = merged_df['timestamp'] - merged_df['timestamp'].min()
    bw = 50 * (1 - (merged_df['lost'] / merged_df['total'])) # convert to bandwidth in Mbps
    tx_bitrate = merged_df['tx_bitrate_mbps']

    fig = plot_bandwidth(t, bw, tx_bitrate)
    fig.savefig(OUT_DIR / "bandwidth_time.pdf", format='pdf', dpi=300)

    # Latency distribution
    latency = latency_df['latency']
    fig = plot_latency_distribution(latency)
    fig.savefig(OUT_DIR / "latency_distribution.pdf", format='pdf', dpi=300)

    # Frame drops
    sidecar = pd.read_csv(DIR / "operator_sidecar.csv")
    t = sidecar['video_timestamp_ns'] * 1e-9
    is_freezed = sidecar['is_repeat']
    fig = plot_frame_drops(t, is_freezed, threshold=2)
    fig.savefig(OUT_DIR / "frame_drops_time.pdf", format='pdf', dpi=300)