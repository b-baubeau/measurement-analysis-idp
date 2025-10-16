#!/bin/python3
import rosbag2_py
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
import cv2
from cv_bridge import CvBridge
import numpy as np
import yaml
from pathlib import Path
import csv

DIR = Path("/media/marcel/TOSHIBA EXT/rosbags")
BAGS = [p for p in DIR.iterdir() if p.is_dir() and not p.name.startswith(".")]
PROCESS_IMAGES = True

NAV_SAT_FIX = "sensor_msgs/msg/NavSatFix"
DIAGNOSTICS = "diagnostic_msgs/msg/DiagnosticArray"
NETWORK_METRICS = "tod_network_monitoring_msgs/msg/NetworkMetrics"
IMAGE = "sensor_msgs/msg/Image"
TARGET_FPS = 40

class DummyHandler:
    def handle_msg(self, msg, t_ns: int):
        pass

class TopicHandlerCsv(DummyHandler):

    def __init__(self, path: Path, topic_name: str, columns: list[str]) -> None:
        self.out_csv = open(path / f"{topic_name.split('/')[-1]}.csv", "w", newline="")
        self.writer = csv.writer(self.out_csv)
        self.writer.writerow(columns)
        self.columns = columns

    def handle_msg(self, msg, t_ns: int):
        data = {}
        for field in self.columns:
            if field == 'timestamp':
                data['timestamp'] = t_ns
            elif hasattr(msg, field):
                data[field] = getattr(msg, field)
            else:
                data[field] = ""
        self.writer.writerow([data.get(col, "") for col in self.columns])

    @staticmethod
    def handler_from_msg(path: Path, topic_name: str, msg) -> 'TopicHandlerCsv':
        columns = ['timestamp'] + [field for field in dir(msg)
                                   if not field.startswith('_') and not field.isupper()
                                   and not callable(getattr(msg, field))]
        return TopicHandlerCsv(path, topic_name, columns)

class DiagnosticArrayHandler(TopicHandlerCsv):
    
    def __init__(self, path: Path, topic_name: str, columns: list[str]) -> None:
        super().__init__(path, topic_name, columns)

    def handle_msg(self, msg, t_ns: int):
        # Collect all key-value pairs from all statuses
        kv_dict = {"timestamp": t_ns}
        for status in msg.status:
            for kv in status.values:
                kv_dict[kv.key] = kv.value
        # Ensure all columns are present in the output
        self.writer.writerow([kv_dict.get(col, "") for col in self.columns])

    @staticmethod
    def handler_from_msg(path: Path, topic_name: str, msg) -> 'DiagnosticArrayHandler':
        # Collect all unique keys from all statuses in the first message
        keys = set()
        for status in msg.status:
            for kv in status.values:
                keys.add(kv.key)
        columns = ["timestamp"] + sorted(keys)
        return DiagnosticArrayHandler(path, topic_name, columns)

class ImageTopicHandler(DummyHandler):
    
    def __init__(self, path: Path, target_fps=30):
        self.bridge = CvBridge()
        self.period_ns = int(round(1e9 / target_fps))
        self.out_video = path / "out.mkv"
        self.out_sidecar = path / "out.csv"
        self.writer = None
        self.frame_idx = 0
        self.next_target_ts_ns = None
        self.last_frame_bgr = None
        self.video_start_ts = None
        self.first_img_seen = False
        self.last_source_ts_ns = None

        self.csv_f = open(self.out_sidecar, "w", newline="")
        self.csv_w = csv.writer(self.csv_f)
        self.csv_w.writerow(["frame_index", "video_timestamp_ns", "source_timestamp_ns", "is_repeat"])
        self.target_fps = target_fps

    def init_writer_if_needed(self, sample_bgr: np.ndarray) -> None:
        if self.writer is None:
            h, w = sample_bgr.shape[:2]
            fourcc = cv2.VideoWriter_fourcc(*"FFV1")  # lossless
            self.writer = cv2.VideoWriter(str(self.out_video), fourcc, self.target_fps, (w, h), isColor=True)
            if not self.writer.isOpened():
                raise RuntimeError("Failed to open VideoWriter with FFV1. Consider PNG sequence fallback.")

    def emit_frozen_frames_until(self, t_img_ns: int) -> int:
        """Emit repeat frames only if there is a gap between last and current image."""
        emitted = 0
        while (
            self.next_target_ts_ns is not None
            and self.next_target_ts_ns < t_img_ns
            and self.last_frame_bgr is not None
        ):
            self.init_writer_if_needed(self.last_frame_bgr)
            self.writer.write(self.last_frame_bgr)
            self.csv_w.writerow([self.frame_idx, self.next_target_ts_ns - self.video_start_ts, self.last_source_ts_ns, 1])  # is_repeat=1
            self.frame_idx += 1
            emitted += 1
            self.next_target_ts_ns += self.period_ns
        return emitted

    def handle_msg(self, msg, t_ns: int):
        img_rgb = self.bridge.imgmsg_to_cv2(msg, desired_encoding="passthrough")
        img_bgr = cv2.cvtColor(img_rgb, cv2.COLOR_RGB2BGR)

        if not self.first_img_seen:
            self.video_start_ts = t_ns
            self.next_target_ts_ns = self.video_start_ts
            self.first_img_seen = True

        # Only emit frozen frames if there is a gap
        self.emit_frozen_frames_until(t_ns)

        # Write the fresh image frame (is_repeat=0)
        self.init_writer_if_needed(img_bgr)
        self.writer.write(img_bgr)
        self.csv_w.writerow([self.frame_idx, t_ns - self.video_start_ts, t_ns, 0])  # is_repeat=0
        self.frame_idx += 1

        # Advance next_target_ts_ns to the next tick after this image
        self.next_target_ts_ns += self.period_ns

        self.last_frame_bgr = img_bgr
        self.last_source_ts_ns = t_ns

    def close(self):
        if self.writer is not None:
            self.writer.release()
        self.csv_f.close()


def process_bag(path):
    print(f"Processing bag at {path}")
    meta = yaml.safe_load((path / "metadata.yaml").read_text())["rosbag2_bagfile_information"]
    storage_identifier = meta["storage_identifier"]

    topics = {t["topic_metadata"]["name"]: t["topic_metadata"]["type"]
              for t in meta["topics_with_message_count"]}
    print(topics)

    storage_options = rosbag2_py.StorageOptions(uri=str(path), storage_id=storage_identifier)
    converter_options = rosbag2_py.ConverterOptions(
        input_serialization_format="cdr",
        output_serialization_format="cdr",
    )

    reader = rosbag2_py.SequentialReader()
    reader.open(storage_options, converter_options)

    handlers = {}

    while reader.has_next():
        (topic, data, t) = reader.read_next()
        msg_type = get_message(next(tt for tt in reader.get_all_topics_and_types() if tt.name == topic).type)
        msg = deserialize_message(data, msg_type)
        
        if topics[topic] == IMAGE and not PROCESS_IMAGES:
            continue
        
        if topic not in handlers:
            if PROCESS_IMAGES and topics[topic] == IMAGE:
                handlers[topic] = ImageTopicHandler(path, target_fps=TARGET_FPS)
            elif topics[topic] == DIAGNOSTICS:
                handlers[topic] = DiagnosticArrayHandler.handler_from_msg(path, topic, msg)
            else:
                handlers[topic] = TopicHandlerCsv.handler_from_msg(path, topic, msg)

        handlers[topic].handle_msg(msg, t)

    for handler in handlers.values():
        if isinstance(handler, ImageTopicHandler):
            handler.close()
    
    print(f"Finished processing bag at {path}")

if __name__ == "__main__":
    for bag_path in BAGS:
        process_bag(bag_path)
    print("All done.")