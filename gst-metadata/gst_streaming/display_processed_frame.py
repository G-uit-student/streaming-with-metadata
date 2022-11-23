# =========================================
# Name_class: DisplayProcessedFrame
# Purpose: This class is ultilized for display 
# purpose from frames contained in display_buffer
# through need_data signal
# =========================================
from common.gst_streaming.appsrc_display import AppSrcDisplay
from common.enums.stream_info import StreamInfo


class DisplayProcessedFrame:
    def __init__(self, camera):
        self.camera = camera
        self.display = AppSrcDisplay(StreamInfo(camera.to_ai_stream_info()))

    def start(self):
        self.display.play()
