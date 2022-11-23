import json
import gi
import cv2
import argparse
import sys
sys.path.append('../yolov5_code')
from detect_yolo import YoloV5

# import required library like Gstreamer and GstreamerRtspServer
gi.require_version('Gst', '1.0')
gi.require_version('GstRtspServer', '1.0')
gi.require_version("GstVideo", "1.0")
gi.require_version("GstRtp", "1.0")
from gi.repository import Gst, GstRtspServer, GObject, GstVideo, GstRtp

GObject.threads_init()
Gst.init(None)
from gst_buffer_info_meta import write_meta, get_meta, map_gst_buffer
import util
from collections import deque 
from queue import Queue
import threading

def pad_probe_callback(pad, info):
    with util.GST_PAD_PROBE_INFO_BUFFER(info) as buffer:
        caps = pad.get_current_caps()
        # print(caps.to_string())
        with map_gst_buffer(buffer,  Gst.MapFlags.WRITE) as mapped:
            ret, buffer = GstRtp.rtp_buffer_map(buffer,Gst.MapFlags.WRITE)
            if q_meta.qsize()==0:
                return Gst.PadProbeReturn.OK
            detection=q_meta.get()

            global frame_id
            _ = buffer.add_extension_twobytes_header(1,1,str(len(detection)).encode('utf-8'))
            _ = buffer.add_extension_twobytes_header(1,2,str(frame_id).encode('utf-8'))
            frame_id+=1
            for i,result in enumerate(detection):
                _ = buffer.add_extension_twobytes_header(1,i+3,result.encode('utf-8'))

    return Gst.PadProbeReturn.OK

class SensorFactory(GstRtspServer.RTSPMediaFactory):
    def set_x_to_rtp_header(self, gst_pad, gst_info,x, y):
        buf = gst_info.get_buffer()
        # print(buf)
        return Gst.PadProbeReturn.OK

    def __init__(self, **properties):
        self.cnt = 0
        super(SensorFactory, self).__init__(**properties)
        # self.cap = cv2.VideoCapture('/home/huycq/Downloads/test.mp4')
        # self.cap = cv2.VideoCapture(0)
        self.number_frames = 0
        self.fps = 10
        self.duration = 1 / self.fps * Gst.SECOND  # duration of a frame in nanoseconds
        self.launch_string = 'appsrc name=source is-live=true block=true format=GST_FORMAT_TIME ' \
                             'caps=video/x-raw,format=BGR,width={},height={},framerate={}/1 ' \
                             '! videoconvert ! video/x-raw,format=I420 ' \
                             '! x264enc speed-preset=ultrafast tune=zerolatency ' \
                             '! rtph264pay config-interval=1 name=pay0 pt=96' \
            .format(640, 480, self.fps)


    def on_need_data(self, src, length):

        if q_frame.qsize()>0:
            frame=q_frame.get()
            buffer = Gst.Buffer.new_wrapped(bytes(frame.data))
            buffer.duration = self.duration
            timestamp = self.number_frames * self.duration
            buffer.pts = buffer.dts = int(timestamp)
            buffer.offset = timestamp
            retval = src.emit('push-buffer', buffer)
            self.number_frames += 1
        # print('pushed buffer, frame {}, duration {} ns, durations {} s'.format(self.number_frames,
                                                                            #    self.duration,
                                                                            #    self.duration / Gst.SECOND))

        if retval != Gst.FlowReturn.OK:
            print(retval)

    # attach the launch string to the override method
    def do_create_element(self, url):
        pipeline = Gst.parse_launch(self.launch_string)
        rtspsrc = pipeline.get_by_name("pay0")
        pad = rtspsrc.get_static_pad("src")
        pad.add_probe(Gst.PadProbeType.BUFFER, pad_probe_callback)
        # gst_object_unref (rtph264pay_src_pad)
        return pipeline

    # attaching the source element to the rtsp media
    def do_configure(self, rtsp_media):
        self.number_frames = 0
        appsrc = rtsp_media.get_element().get_child_by_name('source')
        appsrc.connect('need-data', self.on_need_data)


# Rtsp server implementation where we attach the factory sensor with the stream uri
class GstServer(GstRtspServer.RTSPServer):
    def __init__(self, **properties):
        super(GstServer, self).__init__(**properties)
        self.factory = SensorFactory()
        self.factory.set_shared(True)
        self.set_service(str(8554))
        self.get_mount_points().add_factory("/live", self.factory)
        self.attach(None)
def inference_function():
    while True:
        ret_cap, frame = cap.read()
        if ret_cap:
            _,detection=yolo.detect(frame)
            q_meta.put(detection)
            frame = cv2.resize(frame, (640, 480), \
                                interpolation=cv2.INTER_LINEAR)
            q_frame.put(frame)

            
def main():
    infer_thread = threading.Thread(target=inference_function,daemon=False)
    infer_thread.start()
    server = GstServer()
    loop = GObject.MainLoop()
    loop.run()
if __name__ == '__main__':
    yolo=YoloV5()
    q_meta=Queue()
    q_frame=Queue()
    frame_id=0
    cap = cv2.VideoCapture(0)
    main()

