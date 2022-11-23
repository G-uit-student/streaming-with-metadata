import time
from datetime import datetime
from collections import deque
from abc import abstractmethod
from common.enums.stream_info import StreamInfo
from common.enums.stream_status import StreamStatus


class BaseStreamSource:
    def __init__(self, stream_info):
        self.stream_info = StreamInfo(stream_info)
        self.stream_status = StreamStatus.INIT
        self.stream_message = None
        self.connection_retry = 0
        self.latest_update_status = int(time.time())
        self.queue_logs = deque(maxlen=10)

    def _on_update_status(self, status: StreamStatus, message):
        self.stream_status = status
        self.stream_message = message
        self.latest_update_status = int(time.time())
        if status != StreamStatus.PLAYING:
            self.queue_logs.append({
                'ts': str(datetime.now()),
                'ex': str(message)
            })
        else:
            self.connection_retry = 0
    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass
