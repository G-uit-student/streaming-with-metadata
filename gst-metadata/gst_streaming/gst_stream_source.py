from datetime import datetime

import threading
import time
import uuid
import gi
import sys
from abc import ABC
import numpy as np

if 'gi' in sys.modules:
    gi.require_version('Gst', '1.0')
    from gi.repository import Gst

from .stream_source_abstract import BaseStreamSource
from .rtsp_pipeline import RTSPPipelineSource
from .frame_queue import FrameBufferQueue
from common.enums.stream_status import StreamStatus
from common.enums.streaming_status import StreamingStatus
from common.enums.stream_info import StreamInfo
from common.redis.redis_utils import RedisUtils
from utility.utils_logger import logger
from utility.enums.server_types import ServerTypes


class GstStreamSource(BaseStreamSource, ABC):
    def __init__(self, stream_info, thread_condition: threading.Condition):
        super().__init__(stream_info)
        self._stream = None
        self.thread_condition = thread_condition
        self.stream_buffer = FrameBufferQueue(15)
        self._stream = None
        if self.stream_info.uri is not None:
            self._stream = RTSPPipelineSource(self.stream_info, self._on_new_buffer, self._on_update_status)
        self.last_updated = time.time()
        self.time_delta = time.time() - self.last_updated
        self.frame_cnt = 0
        self.updated_at = datetime.now()
        self.created_at = datetime.now()
        self.app_width, self.app_height = None, None
        self.stream_format = None
        self.stream_fps = None

    def get_stream_status(self):
        return {
            "stream_info": {
                "id": self.stream_info.stream_id,
                "uri": self.stream_info.uri,
                "name": self.stream_info.stream_name
            },
            "stream_format": self.stream_format,
            "stream_size": {
                "height": self.app_height,
                "width": self.app_width
            },
            "stream_status": self.stream_status,
            'updated_at': str(self.updated_at),
            'created_at': str(self.created_at),
            "retry_count": self.connection_retry,
            "decoded_frame_cnt": self.frame_cnt,
            "dropped_frame_cnt": self.stream_buffer.dropped,
            "processed_percent": 0 if self.frame_cnt == 0 else
            float(
                "{:.2f}".format(100 * self.frame_cnt / (self.stream_buffer.dropped + self.frame_cnt))),
            "error_logs": list(self.queue_logs)
            # "average_processed_fps":
        }

    def start(self):
        try:
            # get stream
            camera_by_server = RedisUtils.get_camera_by_server(self.stream_info.stream_id,
                                                               ServerTypes.STREAMING_RELAY_SERVER)

            if camera_by_server is not None and \
                    camera_by_server["main_stream"]["streaming_status"] == StreamingStatus.LIVE:
                self.stream_info.uri = camera_by_server["main_stream"]["local_streaming_rtsp_url"]

                if self._stream is None:
                    self._stream = RTSPPipelineSource(self.stream_info, self._on_new_buffer, self._on_update_status)
                self.connection_retry += 1
                self._stream.play()
            else:
                logger.warn(f"Can not start streaming for AI processing because camera {self.stream_info.stream_id} is not alive")
                self._on_update_status(StreamStatus.ERROR,
                                       'Can not start streaming for AI processing because camera is not alive ')
        except BaseException as ex:
            self._on_update_status(StreamStatus.ERROR, 'Can not start streaming because ' + str(ex))

    def _on_new_buffer(self, sample) -> Gst.FlowReturn.OK:
        buf = sample.get_buffer()
        caps_format = sample.get_caps().get_structure(0)
        self.stream_format = caps_format.get_value('format')
        is_get_fps, numerator, denominator = caps_format.get_fraction('framerate')
        if is_get_fps:
            self.stream_fps = float("{:.2f}".format(numerator / denominator))
        self.app_width, self.app_height = caps_format.get_value('width'), caps_format.get_value('height')
        result, mapinfo = buf.map(Gst.MapFlags.READ)

        if result:
            numpy_frame = np.ndarray(
                shape=(self.app_height, self.app_width, 3),
                dtype=np.uint8,
                buffer=mapinfo.data)
            self.stream_buffer.put({
                "frame": numpy_frame,
                "ts": time.time(),
                "frame_id": str(uuid.uuid4())
            })
            self.frame_cnt += 1
            # self.stream_buffer.put(numpy_frame)
        with self.thread_condition:
            self.thread_condition.notifyAll()
            self.last_updated = time.time()
            self.updated_at = datetime.now()
        buf.unmap(mapinfo)
        return Gst.FlowReturn.OK

    def refresh(self, stream_info):

        self.time_delta = time.time() - self.last_updated
        # logger.info(f"=====> refresh, delta={int(self.time_delta)} stream {stream_info}")
        try:
            self.stream_info = StreamInfo(stream_info)
            if self.stream_info.uri is None:
                logger.warn(f'Can not start {self.stream_info.stream_id} - {self.stream_info.stream_name} because of the URL is None')
                self.stop()
                self._on_update_status(StreamStatus.ERROR,
                                       f'Can not start {self.stream_info.stream_id} - {self.stream_info.stream_name} because of the URL is None')
                return

            if self._stream is None:
                self._stream = RTSPPipelineSource(self.stream_info, self._on_new_buffer, self._on_update_status)

            if self.stream_status != StreamStatus.PLAYING or self.time_delta > 5.0:
                logger.warn(
                    f"=====> Restarting stream {self.stream_info.stream_id} - {self.stream_info.stream_name}, because of {self.stream_status} "
                    f"or delta = {self.time_delta} > 5s")
                self._stream.update_stream_info(self.stream_info)
                self.stop()
                self.start()
        except BaseException as ex:
            logger.exception(
                f'Can not refresh {self.stream_info.stream_id} - {self.stream_info.stream_name} because of {ex}')
            self._on_update_status(StreamStatus.ERROR, str(ex))
            self._stream = None

    def stop(self):
        try:
            if self._stream is not None:
                self._stream.stop()
                self._stream = None
                logger.warn(f'Stopped stream {self.stream_info.stream_id} - {self.stream_info.stream_name}')

        except BaseException as ex:
            logger.exception(
                f'Can not stop {self.stream_info.stream_id} - {self.stream_info.stream_name} because of {ex}')
            self._on_update_status(StreamStatus.ERROR, str(ex))
            self._stream = None
