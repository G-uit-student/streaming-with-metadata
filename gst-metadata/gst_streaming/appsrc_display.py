# Send Numpy image to UDP sink
# gst-launch-1.0 udpsrc port="5000" caps = "application/x-rtp, media=(string)video, clock-rate=(int)90000, encoding-name=(string)RAW, sampling=(string)BGR, width=(string)320, height=(string)240" ! rtpvrawdepay ! videoconvert ! queue ! xvimagesink sync=false
# gst-launch-1.0 -v videotestsrc ! video/x-raw, format="(string)BGR, width=(int)320, height=(int)240, framerate=(fraction)30/1" ! rtpvrawpay ! udpsink host="127.0.0.1" port="5000"
# gst-launch-1.0 rtspsrc location=rtsp://170.93.143.139:1935/rtplive/0b01b57900060075004d823633235daa protocols=4 ! decodebin ! videoconvert ! capsfilter ! video/x-raw, format="(string)BGR, width=(int)320, height=(int)240" ! rtpvrawpay ! udpsink host="224.1.1.1" port="5000" auto-multicast=true
import gi
import sys

if 'gi' in sys.modules:
    gi.require_version('Gst', '1.0')
    from gi.repository import Gst, GLib, GObject
from abc import ABC
from utility.utils_logger import logger as log
from .base_gst_pipeline import BaseGstPipeline
from common.enums.stream_format import StreamFormat
from common.enums.stream_status import StreamStatus
from common.enums.pipeline_status import PipelineStatus
from .frame_queue import FrameBufferQueue
from common.enums.stream_info import StreamInfo

from ai_core.config import DisplayProcessedFrameConfig
from ai_core.config import InputFrame2AiConfig
import cv2


CAPS = "video/x-raw, width=(int){WIDTH}, height=(int){HEIGHT}, framerate=(fraction){FPS}/10, format=(string)BGR"


class AppSrcDisplay(BaseGstPipeline, ABC):
    def __init__(self, 
                 stream_info: StreamInfo,
                 codec_encode: StreamFormat = StreamFormat.RAW_RGB, 
                 fps: int = 15, 
                 pipeline_cmd = None,
                 buffer = None):
        config = DisplayProcessedFrameConfig()
        self.codec = codec_encode                   # Contain format of stream video

        self.display_buffer = None

        self.stream_info = stream_info              # Information of stream that consists of name, pass,... 
        self.pipeline_cmd = self._build_pipeline_cmd(pipeline_cmd)  #-------------------
        self.message = None                                         #-------------------
        super(AppSrcDisplay, self).__init__(self.pipeline_cmd, self._on_update_status) #-------------------
        self.fps = fps                                              #-------------------
        self.duration = 1 / self.fps * Gst.SECOND                   #-------------------
        self.pts = 0                                                #-------------------
        self.latest_frame = None                                    #-------------------
        self.stream_info.width = 640
        self.stream_info.height = 480

        camConfig = InputFrame2AiConfig()
        self.fps = camConfig.processRate


    def _build_pipeline_cmd(self, cmd):
        # if self.codec == StreamFormat.RAW_RGB:
        # RAW format
        if cmd is not None:
            return cmd
        pipeline_cmd = '''  appsrc emit-signals=True is-live=True name=appsrc format=GST_FORMAT_TIME !  
                            queue max-size-buffers=4 !                       
                            autovideoconvert !
                            ximagesink 
                            '''
        return pipeline_cmd.replace('\n', ' ')

    def _on_pipeline_init(self) -> None:
        self.appsrc = self._pipeline.get_by_name('appsrc')
        self.appsrc.set_property("format", Gst.Format.TIME)
        self.appsrc.set_property("block", True)
        appsrc_caps = CAPS.format(
            WIDTH=self.stream_info.width,
            HEIGHT=self.stream_info.height,
            FPS=self.fps
        )
        self.appsrc.set_caps(Gst.Caps.from_string(appsrc_caps))
        assert self.appsrc
        self.appsrc.connect('need-data', self._need_data, None)

    def _need_data(self, appsrc, data, dd):
        frame = None
        # print('need-data', self.display_buffer.qsize())
        try:
            # if self.buffer_queue.qsize() > 0:
            # print(self.buffer_queue.qsize())
            self.latest_frame = self.display_buffer.get()
            self.latest_frame = self.latest_frame['frame'][0]
            
        except BaseException as ex:
            log.error('fsd')

        if self.latest_frame is not None:
            # print(self.latest_frame.shape)
            #     self.latest_frame = ai_result.img.copy()
            #
            # # convert np.ndarray to Gst.Buffer
            # if self.latest_frame is not None:
            cv2.putText(self.latest_frame, self.stream_info.stream_id, (20, 20), cv2.FONT_HERSHEY_SIMPLEX, 1, [225, 255, 255], 3)
            self.latest_frame = cv2.resize(self.latest_frame,(self.stream_info.width,self.stream_info.height))
            delivered_buffer = Gst.Buffer.new_wrapped(bytes(self.latest_frame))

            # set pts and duration to be able to record video, calculate fps
            # self.pts += self.duration
            # delivered_buffer.pts = self.pts
            # delivered_buffer.duration = self.duration
            appsrc.emit("push-buffer", delivered_buffer)

    def _on_update_status(self, pipeline_status: PipelineStatus, message: str):
        self.message = message
        log.info(message)

    def on_error(self, bus: Gst.Bus, message: Gst.Message):
        err, debug = message.parse_error()
        self.log.error("Gstreamer.%s: Error %s: %s. ", self, err, debug)
        self._on_update_status(StreamStatus.ERROR, err)

    def on_eos(self, bus: Gst.Bus, message: Gst.Message):
        self.log.debug("Gstreamer.%s: Received stream EOS event", self)
        self._on_update_status(StreamStatus.EOS, "Got EOS Message")

    def on_warning(self, bus: Gst.Bus, message: Gst.Message):
        warn, debug = message.parse_warning()
        self.log.warning("Gstreamer.%s: %s. %s", self, warn, debug)
        self._on_update_status(StreamStatus.WARNING, warn)
