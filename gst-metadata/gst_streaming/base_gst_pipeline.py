import gi
import sys
import logging
import threading
import typing as typ
from abc import abstractmethod
from common.enums.stream_status import StreamStatus
import gc

if 'gi' in sys.modules:
    gi.require_version('Gst', '1.0')
    gi.require_version('GstBase', '1.0')
    gi.require_version('GstVideo', '1.0')
    gi.require_version('GstApp', '1.0')
    from gi.repository import Gst, GLib, GObject, GstApp, GstVideo  # noqa:F401,F402


class BaseGstPipeline:
    """Base class to initialize any Gstreamer Pipeline from string"""

    def __init__(self, command: str, on_update_status):
        """
        :param command: gst-launch string
        """
        self._command = command
        self._pipeline = None  # Gst.Pipeline
        self._bus = None  # Gst.Bus

        self._log = logging.getLogger("pygst.{}".format(self.__class__.__name__))
        self._log.info("%s \n gst-launch-1.0 %s", self, command)
        self._on_update_status = on_update_status
        self._end_stream_event = threading.Event()

    @property
    def log(self) -> logging.Logger:
        return self._log

    def __str__(self) -> str:
        return self.__class__.__name__

    def __repr__(self) -> str:
        return "<{}>".format(self)

    def __enter__(self):
        self.startup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

    def update_pipeline_cmd(self, pipeline_cmd: str):
        self._command = pipeline_cmd

    def get_by_cls(self, cls: GObject.GType) -> typ.List[Gst.Element]:
        """ Get Gst.Element[] from pipeline by GType """
        elements = self._pipeline.iterate_elements()
        if isinstance(elements, Gst.Iterator):
            # Patch "TypeError: ‘Iterator’ object is not iterable."
            # For versions we have to get a python iterable object from Gst iterator
            _elements = []
            while True:
                ret, el = elements.next()
                if ret == Gst.IteratorResult(1):  # GST_ITERATOR_OK
                    _elements.append(el)
                else:
                    break
            elements = _elements

        return [e for e in elements if isinstance(e, cls)]

    def get_by_name(self, name: str) -> Gst.Element:
        """Get Gst.Element from pipeline by name
        :param name: plugins name (name={} in gst-launch string)
        """
        return self._pipeline.get_by_name(name)

    def startup(self):
        """ Starts pipeline """
        if self._pipeline:
            raise RuntimeError("Can't initiate %s. Already started")
        if self._command is None:
            self._on_update_status(StreamStatus.ERROR, "COMMAND is NONE")
            return
        self._pipeline = Gst.parse_launch(self._command)

        # Initialize Bus
        self._bus = self._pipeline.get_bus()
        self._bus.add_signal_watch()
        self.bus.connect("message::error", self.on_error)
        self.bus.connect("message::eos", self.on_eos)
        self.bus.connect("message::warning", self.on_warning)

        # Initialize Pipeline
        self._on_pipeline_init()
        self._pipeline.set_state(Gst.State.READY)

        self.log.info("Starting %s", self)

        self._end_stream_event.clear()

        self.log.debug(
            "%s Setting pipeline state to %s ... ",
            self,
            'PLAYING',
        )
        self._pipeline.set_state(Gst.State.PLAYING)
        self.log.debug(
            "%s Pipeline state set to %s ", self, 'PLAYING', )

    def _on_pipeline_init(self) -> None:
        """Sets additional properties for plugins in Pipeline"""
        pass

    @property
    def bus(self) -> Gst.Bus:
        return self._bus

    @property
    def pipeline(self) -> Gst.Pipeline:
        return self._pipeline

    def _shutdown_pipeline(self, timeout: int = 1, eos: bool = False) -> None:
        """ Stops pipeline
        :param eos: if True -> send EOS event
            - EOS event necessary for FILE SINK finishes properly
            - Use when pipeline crushes
        """

        if self._end_stream_event.is_set():
            return

        self._end_stream_event.set()

        if not self.pipeline:
            return

        self.log.debug("%s Stopping pipeline ...", self)

        # https://lazka.github.io/pgi-docs/Gst-1.0/classes/Element.html#Gst.Element.get_state
        if self._pipeline.get_state(timeout=1)[1] == Gst.State.PLAYING:
            self.log.info("%s Sending EOS event ...", self)
            try:
                thread = threading.Thread(
                    target=self._pipeline.send_event, args=(Gst.Event.new_eos(),)
                )
                thread.start()
                thread.join(timeout=timeout)
            except Exception as ex:
                self.log.exception(f"=======> exeception stop pipeline {ex}")

        self.log.info("%s Resetting pipeline state ....", self)
        try:

            self._pipeline.set_state(Gst.State.PAUSED)
            self._pipeline.set_state(Gst.State.READY)
            self._pipeline.set_state(Gst.State.NULL)
            del self._pipeline
            self._pipeline = None
        except Exception as ex:
            self.log.exception(f"=======> exeception stop pipeline {ex}")

        gc.collect()
        self.log.debug("%s Gst.Pipeline successfully destroyed", self)
        self._on_update_status(StreamStatus.SHUTDOWN, ' Gst.Pipeline successfully destroyed')

    def shutdown(self, timeout: int = 1, eos: bool = False) -> None:
        """Shutdown pipeline
        :param timeout: time to wait when pipeline fully stops
        :param eos: if True -> send EOS event
            - EOS event necessary for FILE SINK finishes properly
            - Use when pipeline crushes
        """
        self.log.info("%s Shutdown requested ...", self)

        self._shutdown_pipeline(timeout=timeout, eos=eos)

        self.log.info("%s successfully destroyed", self)

    @property
    def is_active(self) -> bool:
        return self.pipeline is not None and not self.is_done

    @property
    def is_done(self) -> bool:
        return self._end_stream_event.is_set()

    @abstractmethod
    def on_error(self, bus: Gst.Bus, message: Gst.Message):
        pass
        # err, debug = message.parse_error()
        # self.log.error("Gstreamer.%s: Error %s: %s. ", self, err, debug)
        # self._shutdown_pipeline()

    @abstractmethod
    def on_eos(self, bus: Gst.Bus, message: Gst.Message):
        pass
        # self.log.debug("Gstreamer.%s: Received stream EOS event", self)
        # self._shutdown_pipeline()

    @abstractmethod
    def on_warning(self, bus: Gst.Bus, message: Gst.Message):
        pass
        # warn, debug = message.parse_warning()
        # self.log.warning("Gstreamer.%s: %s. %s", self, warn, debug)

    def play(self):
        self.startup()
        self._on_update_status(StreamStatus.PLAYING, "start stream successfully")

    def stop(self):
        self._on_update_status(StreamStatus.STOP, "stop stream successfully")
        self.shutdown(1, eos=True)
        self._on_update_status(StreamStatus.EOS, 'stop stream successfully, got eos')
