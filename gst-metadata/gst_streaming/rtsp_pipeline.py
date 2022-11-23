import gi
import sys

if 'gi' in sys.modules:
    gi.require_version('Gst', '1.0')
    gi.require_version('GstBase', '1.0')
    gi.require_version('GstVideo', '1.0')
    gi.require_version('GstApp', '1.0')
    from gi.repository import Gst, GLib, GObject, GstApp, GstVideo  # noqa:F401,F402
from .base_gst_pipeline import BaseGstPipeline
from common.enums.stream_info import StreamInfo
from common.enums.stream_status import StreamStatus
from common.security.security_manager import SecurityManager

H264_RTSP_PIPELINE = "rtspsrc location={location} user-id={username} user-pw={password} protocols=4  ! " \
                     "rtpjitterbuffer ! " \
                     "rtph264depay ! " \
                     "avdec_h264 !  " \
                     "videoconvert ! " \
                     "video/x-raw, format=BGR ! " \
                     "appsink name=appsink sync=false emit-signals=true max-buffers=10 drop=true"

H265_RTSP_PIPELINE = "rtspsrc location={location} user-id={username} user-pw={password} protocols=4 ! " \
                     "rtpjitterbuffer ! " \
                     "rtph265depay ! " \
                     "avdec_h265 !  " \
                     "videoconvert ! video/x-raw, format=BGR ! " \
                     "appsink name=appsink sync=false emit-signals=true max-buffers=10 drop=true"

RTSP_PIPELINE = '''
                rtspsrc location={location} user-id={username} user-pw={password}  protocols=4 ! 
                     rtpjitterbuffer ! 
                     decodebin ! 
                     videoconvert ! 
                     video/x-raw, format=BGR ! 
                     appsink name=appsink sync=false emit-signals=true max-buffers=10 drop=true
                '''


class RTSPPipelineSource(BaseGstPipeline):
    def __init__(self, stream_info: StreamInfo, on_new_buffer, on_update_status):
        self._stream_info = stream_info
        self._pipeline_cmd = self._build_pipeline_cmd()
        super(RTSPPipelineSource, self).__init__(self._pipeline_cmd, on_update_status)
        self.on_new_buffer = on_new_buffer
        self.appsink = None

    def _build_pipeline_cmd(self):
        # if self._stream_info.fmt == StreamFormat.H265:
        #     pipeline_cmd = H265_RTSP_PIPELINE.format(
        #         location=self._stream_info.uri,
        #         username=self._stream_info.username,
        #         password=self._stream_info.password
        #     )
        # else: #defaul is H264
        #     pipeline_cmd = H264_RTSP_PIPELINE.format(
        #         location=self._stream_info.uri,
        #         username=self._stream_info.username,
        #         password=self._stream_info.password
        #     )
        pipeline_cmd = RTSP_PIPELINE.format(
            location=self._stream_info.uri + '?at=' + SecurityManager.INTERNAL_ACCESS_TOKEN,
            username=self._stream_info.username,
            password=self._stream_info.password)
        return pipeline_cmd.replace('\n', '')

    def _on_pipeline_init(self) -> None:
        # init data
        self.appsink = self._pipeline.get_by_name('appsink')
        assert self.appsink
        self.appsink.connect('new-sample', self._on_new_sample, None)

    def _on_new_sample(self, sink, data) -> Gst.FlowReturn.OK:
        sample = sink.emit('pull-sample')
        self.on_new_buffer(sample)
        return Gst.FlowReturn.OK

    def on_error(self, bus: Gst.Bus, message: Gst.Message):
        err, debug = message.parse_error()
        self.log.error("[%s] - [%s] - Gstreamer.%s: Error %s: %s. ", self._stream_info.stream_id,
                       self._stream_info.stream_name, self, err, debug)
        self._on_update_status(StreamStatus.ERROR, err)
        self.shutdown()
        self._on_update_status(StreamStatus.SHUTDOWN, "Shutdown because Got ERROR Message")

    def on_eos(self, bus: Gst.Bus, message: Gst.Message):
        self.log.error("[%s] - [%s] - Gstreamer.%s: Received stream EOS event", self._stream_info.stream_id,
                       self._stream_info.stream_name, self)
        self._on_update_status(StreamStatus.EOS, "Got EOS Message")
        self.shutdown()
        self._on_update_status(StreamStatus.SHUTDOWN, "Shutdown because Got EOS Message")

    def on_warning(self, bus: Gst.Bus, message: Gst.Message):
        warn, debug = message.parse_warning()
        self.log.warning("[%s] - [%s] Gstreamer.%s: %s. %s",self._stream_info.stream_id,
                         self._stream_info.stream_name, self, warn, debug)
        self._on_update_status(StreamStatus.WARNING, warn)

    def update_stream_info(self, stream_info: StreamInfo):
        self._stream_info = stream_info
        self._pipeline_cmd = self._build_pipeline_cmd()
        self.update_pipeline_cmd(self._pipeline_cmd)
