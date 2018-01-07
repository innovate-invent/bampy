import ctypes as C
from ctypes import util
import platform

# Special thanks to Mark Nottingham https://gist.github.com/mnot/242459
# and the zlib example source for reference implementations.

# Constants taken from zlib.h
MAX_WBITS = 15
ZLIB_VERSION = C.c_char_p(b"1.2.3")

# Allowed flush values; see deflate() and inflate()
Z_NO_FLUSH = 0
Z_PARTIAL_FLUSH = 1
Z_SYNC_FLUSH = 2
Z_FULL_FLUSH = 3
Z_FINISH = 4
Z_BLOCK = 5
Z_TREES = 6

# Return codes for the compression/decompression functions. Negative values
# are errors, positive values are used for special but normal events.
Z_OK = 0
Z_STREAM_END = 1
Z_NEED_DICT = 2
Z_ERRNO = -1
Z_STREAM_ERROR = -2
Z_DATA_ERROR = -3
Z_MEM_ERROR = -4
Z_BUF_ERROR = -5
Z_VERSION_ERROR = -6

# compression levels
Z_NO_COMPRESSION = 0
Z_BEST_SPEED = 1
Z_BEST_COMPRESSION = 9
Z_DEFAULT_COMPRESSION = -1

# compression strategy; see deflateInit2()
Z_FILTERED = 1
Z_HUFFMAN_ONLY = 2
Z_RLE = 3
Z_FIXED = 4
Z_DEFAULT_STRATEGY = 0

# Possible values of the data_type field for deflate()
Z_BINARY = 0
Z_TEXT = 1
Z_ASCII = Z_TEXT   # for compatibility with 1.2.2 and earlier
Z_UNKNOWN = 2

# The deflate compression method (the only one supported in this version)
Z_DEFLATED = 8


Z_NULL = 0  #  for initializing zalloc, zfree, opaque

if platform.system() == 'Windows':
    path = util.find_library("zlib1.dll")
    if not path:
        import os
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'zlibwapi.dll')
    _zlib = C.windll.LoadLibrary(path)
else:
    _zlib = C.cdll.LoadLibrary(util.find_library("z"))

class zState(C.Structure):
    _fields_ = [
        ("next_in", C.POINTER(C.c_ubyte)),
        ("avail_in", C.c_uint),
        ("total_in", C.c_ulong),
        ("next_out", C.POINTER(C.c_ubyte)),
        ("avail_out", C.c_uint),
        ("total_out", C.c_ulong),
        ("msg", C.c_char_p),
        ("state", C.c_void_p),
        ("zalloc", C.c_void_p),
        ("zfree", C.c_void_p),
        ("opaque", C.c_void_p),
        ("data_type", C.c_int),
        ("adler", C.c_ulong),
        ("reserved", C.c_ulong),
    ]

def raw_compress(src, dest, level=6, wbits=MAX_WBITS, mode=Z_FINISH, memlevel=8, dictionary=None, state=None) -> (int, zState):
    if state:
        err = Z_OK
    else:
        state = zState()
        state.next_in = C.byref(src)
        state.avail_in = len(src)
        state.next_out = C.byref(dest)
        state.avail_out = Z_NULL
        err = _zlib.deflateInit2_(C.byref(state), level, Z_DEFLATED, -wbits, memlevel, Z_DEFAULT_STRATEGY, ZLIB_VERSION, C.sizeof(zState))
        if err == Z_OK and dictionary:
            err = _zlib.deflateSetDictionary(C.byref(state), C.cast(C.c_char_p(dictionary), C.POINTER(C.c_ubyte)), len(dictionary))

    if err == Z_OK:
        _zlib.deflate(C.byref(state), mode)

    if err == Z_STREAM_END:
        return _zlib.deflateEnd(C.byref(state)), None

    return err, state

def raw_decompress(src, dest, wbits=MAX_WBITS, mode=Z_FINISH, dictionary=None, state=None) -> (int, zState):
    if state:
        err = Z_OK
    else:
        state = zState()
        state.next_in = C.cast(C.pointer(src), C.POINTER(C.c_ubyte))
        state.avail_in = len(src)
        state.next_out = C.cast(C.pointer(dest), C.POINTER(C.c_ubyte))
        state.avail_out = len(dest)
        err = _zlib.inflateInit2_(C.byref(state), -wbits, ZLIB_VERSION, C.sizeof(zState))

    if err == Z_OK:
        _zlib.inflate(C.byref(state), mode)
        if err == Z_NEED_DICT:
            assert dictionary, err
            err = _zlib.inflateSetDictionary(C.byref(state), C.cast(C.c_char_p(dictionary), C.POINTER(C.c_ubyte)), len(dictionary))

    if err == Z_STREAM_END:
        return _zlib.inflateEnd(C.byref(state)), None

    return err, state

def crc32(src):
    return _zlib.crc32(_zlib.crc32(0, Z_NULL, 0), src, len(src))