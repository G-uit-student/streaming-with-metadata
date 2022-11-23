
import argparse
import os
import sys
from pathlib import Path
import cv2
import numpy as np
import torch
import torch.backends.cudnn as cudnn
from utils.augmentations import letterbox

FILE = Path(__file__).resolve()
ROOT = FILE.parents[0]  # YOLOv5 root directory
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))  # add ROOT to PATH
ROOT = Path(os.path.relpath(ROOT, Path.cwd()))  # relative

from models.experimental import attempt_load
from utils.datasets import LoadImages, LoadStreams
from utils.general import apply_classifier, check_img_size, check_imshow, check_requirements, check_suffix, colorstr, \
    increment_path, non_max_suppression, print_args, save_one_box, scale_coords, set_logging, \
    strip_optimizer, xyxy2xywh
from utils.plots import Annotator, colors
from utils.torch_utils import load_classifier, select_device, time_sync
import threading
import queue


class YoloV5(object):
    def __init__(self,weights='../yolov5_code/s_640.pt', device='', conf_thres=0.25, iou_thres=0.45,max_det=1000): 
        self.device = select_device(device)
        self.model = attempt_load(weights, map_location=self.device)
        self.names = self.model.module.names if hasattr(self.model, 'module') else self.model.names
        self.img_size = 640
        self.stride = int(self.model.stride.max())
        self.half=False
        self.conf_thres=conf_thres
        self.iou_thres=iou_thres
        self.max_det=max_det
        self.visualize=True
        self.c_processed_frame  = threading.Condition()  
        self.processed_backbone_buffer   = queue.Queue(maxsize=5)
        
        
    def detect(self,img0):
        
        img0,img=self.preprocess(img0)
        img = torch.from_numpy(img).to(self.device)
        img = img.half() if self.half else img.float()  # uint8 to fp16/32
        img = img / 255.0  # 0 - 255 to 0.0 - 1.0
        if len(img.shape) == 3:
            img = img[None] 
        with torch.no_grad():
            pred = self.model(img, augment=False, visualize=False)[0]
        im0,results=self.postprocess(pred,img0,img)
        
        return im0, results

    def postprocess(self,pred,im0s,img):
        pred = non_max_suppression(pred, self.conf_thres, self.iou_thres, None, False, max_det=self.max_det)
        results=[]
        for i, det in enumerate(pred):  # per image
            im0= im0s.copy()
            gn = torch.tensor(im0.shape)[[1, 0, 1, 0]]  # normalization gain whwh
            if self.visualize:
                annotator = Annotator(im0, line_width=3, example=str(self.names))
            
            if len(det):
                # Rescale boxes from img_size to im0 size
                det[:, :4] = scale_coords(img.shape[2:], det[:, :4], im0.shape).round()

                # Print results
                for c in det[:, -1].unique():
                    n = (det[:, -1] == c).sum() 
                

                for *xyxy, conf, cls in reversed(det):
                    if self.visualize:  # Add bbox to image
                        c = int(cls)  # integer class
                        label = (f'{self.names[c]} {conf:.2f}')
                        annotator.box_label(xyxy, label, color=colors(c, True))
                    xywh = (xyxy2xywh(torch.tensor(xyxy).view(1, 4)) / gn).view(-1).tolist()
                    line = (cls, *xywh)
                    results.append(str(('%g ' * len(line)).rstrip() % line))
                # print(results) 
            if self.visualize:  
                im0 = annotator.result()
        return im0,results
    
    def preprocess(self,img0):
        img = letterbox(img0, self.img_size, stride=self.stride, auto=True)[0]
        img = img.transpose((2, 0, 1))[::-1]  # HWC to CHW, BGR to RGB
        img = np.ascontiguousarray(img)
        return img0,img
        
# yolo=YoloV5()
# cap = cv2.VideoCapture('/home/huycq/yolov5/test_video.mp4')

# if (cap.isOpened()== False): 
#     print("Error opening video stream or file")
# end_t,start_t=5,5
# while(cap.isOpened()):
#     ret, frame = cap.read()
#     if ret == True:
        
#         img,detection=yolo.detect(frame)
#         cv2.imshow('Frame',img)
#         if cv2.waitKey(1) & 0xFF == ord('q'):
#             break
#     else: 
#         break


# cap.release()
# cv2.destroyAllWindows()