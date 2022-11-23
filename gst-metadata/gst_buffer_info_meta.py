from copy import deepcopy

import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GObject
from ctypes import *
from contextlib import contextmanager
import sys
from ctypes import *
Gst.init(None)

from ctypes import *

GST_PADDING = 4
class GstMapInfo(Structure):
    _fields_ = [("memory", c_void_p),        # GstMemory *memory
                ("flags", c_int),            # GstMapFlags flags
                ("data", POINTER(c_byte)),   # guint8 *data
                ("size", c_size_t),          # gsize size
                ("maxsize", c_size_t),       # gsize maxsize
                ("user_data", c_void_p * 4), # gpointer user_data[4]
                ("_gst_reserved", c_void_p * GST_PADDING)]

libgst = CDLL("libgstreamer-1.0.so.0")
# gst_buffer_map
GST_MAP_INFO_POINTER = POINTER(GstMapInfo)


libgst.gst_buffer_map.argtypes = [c_void_p, GST_MAP_INFO_POINTER, c_int]
libgst.gst_buffer_map.restype = c_int

# gst_buffer_unmap
libgst.gst_buffer_unmap.argtypes = [c_void_p, GST_MAP_INFO_POINTER]
libgst.gst_buffer_unmap.restype = None

# gst_mini_object_is_writable
libgst.gst_mini_object_is_writable.argtypes = [c_void_p]
libgst.gst_mini_object_is_writable.restype = c_int

@contextmanager
def map_gst_buffer(pbuffer, flags):
    if pbuffer is None:
        raiseTypeError("Cannot pass NULL to _map_gst_buffer")

    ptr = hash(pbuffer)
    if flags & Gst.MapFlags.WRITE and libgst.gst_mini_object_is_writable(ptr) == 0:
        raiseValueError("Writable array requested but buffer is not writeable")

    mapping = GstMapInfo()
    success = libgst.gst_buffer_map(ptr, mapping, flags)

    if not success:
        raiseRuntimeError("Couldn't map buffer")

    try:
        yield cast(mapping.data, POINTER(c_byte * mapping.size)).contents
    finally:
        libgst.gst_buffer_unmap(ptr, mapping)

# Metadata structure that describes GstBufferInfo (C)
class GstBufferInfo(Structure):
    _fields_ = [("description", c_char_p)]

# Pointer to GstBufferInfo structure
GstBufferInfoPtr = POINTER(GstBufferInfo)

# Load C-lib
clib = CDLL("build/libgst_buffer_info_meta.so")

# Map ctypes arguments to C-style arguments
clib.gst_buffer_add_buffer_info_meta.argtypes = [c_void_p, GstBufferInfoPtr]
clib.gst_buffer_add_buffer_info_meta.restype = c_void_p

clib.gst_buffer_get_buffer_info_meta.argtypes = [c_void_p]
clib.gst_buffer_get_buffer_info_meta.restype = GstBufferInfoPtr

clib.gst_buffer_remove_buffer_info_meta.argtypes = [c_void_p]
clib.gst_buffer_remove_buffer_info_meta.restype = c_bool


def write_meta(buffer, description):
    """
        Writes GstBufferInfo as metadata to Gst.Buffer

        :param name: buffer
        :type name: Gst.Buffer

        :param name: custom information to be written
        :type name: str
    """
    with map_gst_buffer(buffer, Gst.MapFlags.WRITE) as mapped:
        meta = GstBufferInfo()
        meta.description = description.encode("utf-8")
        clib.gst_buffer_add_buffer_info_meta(hash(buffer), meta)


def get_meta(buffer):
    """
        Get GstBufferInfo from Gst.Buffer

        :param name: buffer
        :type name: Gst.Buffer

        :rtype: GstBufferInfo
    """  
    res = clib.gst_buffer_get_buffer_info_meta(hash(buffer))
    return res.contents


def remove_meta(buffer):
    """
        Removes GstBufferInfo from Gst.Buffer

        :param name: buffer
        :type name: Gst.Buffer

        :rtype: bool
    """  
    return clib.gst_buffer_remove_buffer_info_meta(hash(buffer))
