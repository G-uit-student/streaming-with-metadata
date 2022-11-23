from abc import ABC
import gi
import sys
import time

if 'gi' in sys.modules:
    gi.require_version('Gst', '1.0')
    from gi.repository import Gst
from datetime import datetime
from .base_gst_pipeline import BaseGstPipeline
from common.enums.stream_info import StreamInfo
from common.enums.stream_format import StreamFormat
from common.enums.stream_status import StreamStatus

RECORDING_PIPELINE = '''
                        rtspsrc location={RTSP_URL} protocols=4 ! 
                        rtpjitterbuffer ! 
                        {RTP_DEPAY} !                         
                        splitmuxsink name=splitmuxsink0 max-size-time={DURATION}  muxer=mpegtsmux 
                    '''


class RecordingPipeline(BaseGstPipeline, ABC):
    def __init__(self, stream_info, fragment_duration, recording_location, on_update_status):
        self._stream_info = StreamInfo(stream_info)
        self._splitmuxsink = None
        self.fragment_duration = fragment_duration
        self.current_recording_file = None
        self.current_recording_file_index = 0
        self.recording_location = recording_location
        self._pipeline_cmd = self._build_pipeline_cmd()
        super(RecordingPipeline, self).__init__(self._pipeline_cmd, on_update_status)

    def _build_pipeline_cmd(self):

        if self._stream_info.fmt == StreamFormat.H265:
            pipeline_cmd = RECORDING_PIPELINE.format(
                RTSP_URL=self._stream_info.uri,
                RTP_DEPAY='rtph265depay',
                DURATION=self.fragment_duration
            )
        elif self._stream_info.fmt == StreamFormat.H264:
            pipeline_cmd = RECORDING_PIPELINE.format(
                RTSP_URL=self._stream_info.uri,
                RTP_DEPAY='rtph264depay',
                DURATION=self.fragment_duration
            )
        else:
            pipeline_cmd = RECORDING_PIPELINE.format(
                RTSP_URL=self._stream_info.uri,
                RTP_DEPAY='rtph264depay',
                DURATION=self.fragment_duration
            )
        print(pipeline_cmd.replace('\n', ''))
        return pipeline_cmd.replace('\n', '')

    def _on_pipeline_init(self):
        self._splitmuxsink = self._pipeline.get_by_name('splitmuxsink0')
        assert self._splitmuxsink
        self._splitmuxsink.connect('format-location', self._on_format_location, None)

    def _on_format_location(self, sink, id, data):
        self.last_update_status = time.time()
        self._on_update_status(StreamStatus.PLAYING, "New Video file", self.current_recording_file)
        self.current_recording_file = self.recording_location + '/' + datetime.now().strftime(
            '%Y-%m-%d-%H-%M-%S') + '_' + '%010d' % id + '.ts'
        self.current_recording_file_index = id
        return self.current_recording_file

    def on_error(self, bus: Gst.Bus, message: Gst.Message):
        err, debug = message.parse_error()
        self.log.error("Gstreamer.%s: Error %s: %s. ", self, err, debug)
        self._on_update_status(StreamStatus.ERROR, err)
        self.shutdown()
        self._on_update_status(StreamStatus.SHUTDOWN, "Shutdown because Got ERROR Message")

    def on_eos(self, bus: Gst.Bus, message: Gst.Message):
        self.log.debug("Gstreamer.%s: Received stream EOS event", self)
        self._on_update_status(StreamStatus.EOS, "Got EOS Message")
        self.shutdown()
        self._on_update_status(StreamStatus.SHUTDOWN, "Shutdown because Got EOS Message")

    def update_stream_info(self, stream_info):
        self._stream_info = stream_info
        self._pipeline_cmd = self._build_pipeline_cmd()
        self.update_pipeline_cmd(self._pipeline_cmd)
