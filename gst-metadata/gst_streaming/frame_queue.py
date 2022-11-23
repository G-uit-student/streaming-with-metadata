import queue
import typing as typ


class FrameBufferQueue(queue.Queue):
    """Queue that contains only the last actual items and drops the oldest one."""

    def __init__(
            self,
            maxsize: int = 100,
            on_drop: typ.Optional[typ.Callable[["LeakyQueue", "object"], None]] = None,
    ):
        super().__init__(maxsize=maxsize)
        self._dropped = 0
        self._on_drop = on_drop or (lambda queue, item: None)

    def put(self, item, block=False, timeout=None):
        if self.full():
            dropped_item = self.get_nowait()
            self._dropped += 1
            self._on_drop(self, dropped_item)
            # print('#stream_id=%s full' % dropped_item.stream_id)
        super().put(item, block=block, timeout=timeout)

    def clear(self):
        with self.mutex:
            self.queue.clear()

    @property
    def dropped(self):
        return self._dropped
