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
# results=[]
# arr=None

  
# Initializing a queue
q = Queue()
q2 = Queue()
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
    # global results
    # global arr
    # print(stream_fps,app_height,app_width)
    arr = np.ndarray(
            (app_height,
             app_width,
             3),
            buffer=buf.extract_dup(0, buf.get_size()),
            dtype=np.uint8)
    
    q.put(arr)
    return Gst.FlowReturn.OK

def pad_probe_callback(pad, info):
    with util.GST_PAD_PROBE_INFO_BUFFER(info) as buffer:
        caps = pad.get_current_caps()
        # with map_gst_buffer(buffer,  Gst.MapFlags.WRITE) as mapped:
        ret, buffer = GstRtp.rtp_buffer_map(buffer,Gst.MapFlags.WRITE)
        ret1,appbits, len_ = buffer.get_extension_twobytes_header(1,0)
        # print("ret",ret)
        ret2,appbits, frame_id = buffer.get_extension_twobytes_header(2,0)
        # global arr
        results=[]
        len_=len_.decode("utf-8")
        if ret1==False or ret2 ==False:
            # q2.put(results)
            return Gst.PadProbeReturn.OK
        # arr=q.get()
        
        # print(len_)
        # print(type(len_),int(len_))
        # print(len_.decode("utf-8"))
        # results=[]
        for i in range(int(len_)):
            data2,appbits, result = buffer.get_extension_twobytes_header(i+3,0)
            # print(result)
            arrs=str(result.decode("utf-8")).split(' ')
            if len(arrs)<3:
                continue
            results.append(arrs)
        q2.put([results,frame_id.decode("utf-8")])
                # print(arrs)
            #     if len(arrs)<3:
            #         continue
            #     cls=int(arrs[0])
            #     xc=float(arrs[1])
            #     yc=float(arrs[2])
            #     w=float(arrs[3])
            #     h=float(arrs[4])
            #     x1,y1,x2,y2=int(float(xc-w/2)*width),int(float(yc-h/2)*height),int(float(xc+w/2)*width),int(float(yc+h/2)*height)
            #     arr = cv2.rectangle(arr, (x1,y1), (x2,y2), (255,0,0), 2)
            # cv2.imshow("buf",arr)
            # cv2.waitKey(1)
            # print(q.qsize(), q2.qsize())
            # if q.qsize()>0 and q2.qsize()>0:
            #     img=q.get()
            #     height,width,_=img.shape
            #     results_=q2.get()
            #     for arrs in results_:
            #         cls=int(arrs[0])
            #         xc=float(arrs[1])
            #         yc=float(arrs[2])
            #         w=float(arrs[3])
            #         h=float(arrs[4])
            #         x1,y1,x2,y2=int(float(xc-w/2)*width),int(float(yc-h/2)*height),int(float(xc+w/2)*width),int(float(yc+h/2)*height)
            #         img = cv2.rectangle(img, (x1,y1), (x2,y2), (255,0,0), 2)
            #     cv2.imshow("buf",img)
            #     cv2.waitKey(1)
            # data2,appbits, meta2 = buffer.get_extension_twobytes_header(3,0)
            # if data1:
            #     global bit
            #     bit = meta




    return Gst.PadProbeReturn.OK
def show_func():
    num=0
    out = cv2.VideoWriter('outpy.avi',cv2.VideoWriter_fourcc('M','J','P','G'), 10, (640,480))
    while True:
        # print(q.qsize(), q2.qsize())
        if q.qsize()>0 and q2.qsize()>0:
            img=q.get()
            results_,frame_id=q2.get()
            # print(num,frame_id,img[0,0,0])
            
            height,width,_=img.shape
            
            for arrs in results_:
                cls=int(arrs[0])
                xc=float(arrs[1])
                yc=float(arrs[2])
                w=float(arrs[3])
                h=float(arrs[4])
                x1,y1,x2,y2=int(float(xc-w/2)*width),int(float(yc-h/2)*height),int(float(xc+w/2)*width),int(float(yc+h/2)*height)
                img = cv2.putText(img, "frame id metadata :"+str(frame_id), (50, 150), cv2.FONT_HERSHEY_SIMPLEX, 
                   1, (255, 0, 0), 2, cv2.LINE_AA)
                img = cv2.putText(img, "counter from client :"+str(num), (50, 250), cv2.FONT_HERSHEY_SIMPLEX, 
                   1, (255, 0, 0), 2, cv2.LINE_AA)
                img = cv2.rectangle(img, (x1,y1), (x2,y2), (255,0,0), 2)
            out.write(img)
            num+=1
            cv2.imshow("buf",img)
            cv2.waitKey(1)
    out.release()
            # if k==ord(' '):
            #     while k==ord(' '):
            #         k=cv2.waitKey(1)
if __name__=="__main__":
    main_loop = GLib.MainLoop()
    pipeline = Gst.parse_launch(""" rtspsrc location=rtsp://127.0.0.1:8554/live name=xxx ! rtph264depay name=depay ! decodebin ! videoconvert ! video/x-raw, format=BGR !  appsink sync=true     max-buffers=1 drop=true name=sink emit-signals=true""")
    appsink = pipeline.get_by_name("sink")
    depay = pipeline.get_by_name("depay")
    pad = depay.get_static_pad("sink")
    pad.add_probe(Gst.PadProbeType.BUFFER, pad_probe_callback)
    
    pipeline.set_state(Gst.State.PLAYING)
    handler_id = appsink.connect("new-sample", on_new_sample)
    show_thread = threading.Thread(target=show_func,daemon=False)
    show_thread.start()
    time.sleep(50)
    # print("here")
    pipeline.set_state(Gst.State.NULL)
    # print("hrer")
    main_loop.quit()
    # print("here")
