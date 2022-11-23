import json
import time
import gi
import numpy as np
import cv2

gi.require_version("Gst", "1.0")
gi.require_version("GstApp", "1.0")
gi.require_version("GstVideo", "1.0")
gi.require_version("GstRtp", "1.0")
from gi.repository import Gst, GstApp, GLib, GstVideo, GstRtp
from queue import Queue
import threading
# import queue
Gst.init(None)
_ = GstApp
import util
import gst_buffer_info_meta
from gst_buffer_info_meta     import get_meta, map_gst_buffer, write_meta


  
# Initializing a queue

def on_new_sample(app_sink):

    sample = app_sink.emit('pull-sample')
    # print("new sample")
    buf = sample.get_buffer()
    # wrap buf to numpy
    caps_format = sample.get_caps().get_structure(0)
    stream_format = caps_format.get_value('format')
    is_get_fps, numerator, denominator = caps_format.get_fraction('framerate')
    stream_fps = float("{:.2f}".format(numerator / denominator))
    app_width, app_height = caps_format.get_value('width'), caps_format.get_value('height')

    arr = np.ndarray(
            (app_height,
             app_width,
             3),
            buffer=buf.extract_dup(0, buf.get_size()),
            dtype=np.uint8)
    
    q_frame.put(arr)
    return Gst.FlowReturn.OK

def pad_probe_callback(pad, info):
    with util.GST_PAD_PROBE_INFO_BUFFER(info) as buffer:
        caps = pad.get_current_caps()
        ret, buffer = GstRtp.rtp_buffer_map(buffer,Gst.MapFlags.WRITE)
        ret1,appbits, len_ = buffer.get_extension_twobytes_header(1,0)
        ret2,appbits, frame_id = buffer.get_extension_twobytes_header(2,0)
        results=[]
        len_=len_.decode("utf-8")
        if ret1==False or ret2 ==False:
            return Gst.PadProbeReturn.OK

        for i in range(int(len_)):
            data2,appbits, result = buffer.get_extension_twobytes_header(i+3,0)
            # print(result)
            arrs=str(result.decode("utf-8")).split(' ')
            if len(arrs)<3:
                continue
            results.append(arrs)
        q_metadata.put([results,frame_id.decode("utf-8")])





    return Gst.PadProbeReturn.OK
def show_func():
    num=0
    out = cv2.VideoWriter('outpy.avi',cv2.VideoWriter_fourcc('M','J','P','G'), 10, (640,480))
    while True:
        # print(q.qsize(), q2.qsize())
        if q_frame.qsize()>0 and q_metadata.qsize()>0:
            img=q_frame.get()
            results_,frame_id=q_metadata.get()
            
            height,width,_=img.shape
            
            for arrs in results_:
                cls=int(arrs[0])
                xc=float(arrs[1])
                yc=float(arrs[2])
                w=float(arrs[3])
                h=float(arrs[4])
                x1,y1,x2,y2=int((xc-w/2)*width),int((yc-h/2)*height),int((xc+w/2)*width),int((yc+h/2)*height)
                # img = cv2.putText(img, "frame id metadata :"+str(frame_id), (50, 150), cv2.FONT_HERSHEY_SIMPLEX, 
                #    1, (255, 0, 0), 2, cv2.LINE_AA)
                img = cv2.putText(img, names[int(cls)], (x1-20,y1), cv2.FONT_HERSHEY_SIMPLEX, 
                   1, (255, 0, 0), 1, cv2.LINE_AA)
                img = cv2.rectangle(img, (x1,y1), (x2,y2), (0,0,0), 2)
            out.write(img)
            num+=1
            cv2.imshow("buf1",img)
            cv2.waitKey(1)
    out.release()
if __name__=="__main__":
    q_frame = Queue()
    q_metadata = Queue()
    names=[
        'person',
        'motorbike',
        'bicycle',
        'face',
        'plate',
        'longplate',
        'car',
        'truck',
        'van',
        'bus',
        'bagac'
    ]
    main_loop = GLib.MainLoop()
    pipeline = Gst.parse_launch(""" rtspsrc location=rtsp://127.0.0.1:1554/cam name=xxx ! rtph264depay name=depay ! decodebin ! videoconvert ! video/x-raw, format=BGR !  appsink sync=true     max-buffers=1 drop=true name=sink emit-signals=true""")
    appsink = pipeline.get_by_name("sink")
    depay = pipeline.get_by_name("depay")
    pad = depay.get_static_pad("sink")
    pad.add_probe(Gst.PadProbeType.BUFFER, pad_probe_callback)
    
    pipeline.set_state(Gst.State.PLAYING)
    handler_id = appsink.connect("new-sample", on_new_sample)
    show_thread = threading.Thread(target=show_func,daemon=False)
    show_thread.start()
    time.sleep(50)
    pipeline.set_state(Gst.State.NULL)
    main_loop.quit()
