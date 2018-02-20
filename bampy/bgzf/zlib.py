"""
Provides a basic wrapper for the zlib library.

This uses ctypes to load the zlib dll from the system.
If this is run on a Windows system and ctypes.util.find() can not find zlibwapi.dll it will look in the folder this
code is stored in.
"""

# TODO inflateReset/deflateReset may speed up [de]compression
# TODO inflateBack avoids a copy during decompression

import ctypes as C
import platform
from ctypes import util

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
Z_ASCII = Z_TEXT  # for compatibility with 1.2.2 and earlier
Z_UNKNOWN = 2

# The deflate compression method (the only one supported in this version)
Z_DEFLATED = 8

Z_NULL = 0  # for initializing zalloc, zfree, opaque

NULL_PTR = C.cast(0, C.POINTER(C.c_ubyte))

if platform.system() == 'Windows':
    path = util.find_library("zlib1.dll")
    if not path:
        import os

        path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'zlibwapi.dll')
    _zlib = C.windll.LoadLibrary(path)
else:
    _zlib = C.cdll.LoadLibrary(util.find_library("z"))


class zState(C.Structure):
    """
    Represents the zlib internal state object used during inflate and deflate
    :ivar next_in: C._Pointer   next input byte
    :ivar avail_in: C.c_uint    number of bytes available at next_in
    :ivar total_in: C.c_ulong   total number of input bytes read so far
    :ivar next_out: C._Pointer  next output byte will go here
    :ivar avail_out: C.c_uint   remaining free space at next_out
    :ivar total_out: C.c_ulong  total number of bytes output so far
    :ivar msg: C.c_char_p       last error message, NULL if no error
    :ivar state: C.c_void_p     used to allocate the internal state
    :ivar zalloc: C.c_void_p    used to free the internal state
    :ivar zfree: C.c_void_p     private data object passed to zalloc and zfree
    :ivar opaque: C.c_void_p    best guess about the data type: binary or text
    :ivar data_type: C.c_int    for deflate, or the decoding state for inflate
    :ivar adler: C.c_ulong      Adler-32 or CRC-32 value of the uncompressed data
    :ivar reserved: C.c_ulong   reserved for future use
    """
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


SIZEOF_ZSTATE = C.sizeof(zState)


def raw_compress(src=None, dest=None, mode=Z_FINISH, state=None, level=8, wbits=MAX_WBITS, memlevel=8, dictionary=None) -> (int, zState):
    """
    Wraps zlib.deflate().
    Manages compression state and conversion to ctypes compatible types.
    Set src and dest to None to preserve their values between calls.
    For example, on the initial call set src to some input data and dest to an output buffer.
    On subsequent calls until the output buffer is full, leave dest set to None and src a new buffer of data to compress.
    :param src: Data buffer to compress or None.
    :param dest: Data buffer for compressed data to be written to or None.
    :param mode: Compression flush mode. Use Z_NO_FLUSH, Z_PARTIAL_FLUSH, or Z_FINISH. See zlib documentation for full description.
    :param state: State instance returned from previous call or None on first call.
    :param level: zlib algorithm compression level from 0-9. Default=8. Pass -1 for zlib default.
    :param wbits: Compression window bit size. Defaults to MAX_WBITS, do not change unless you REALLY know what you are doing.
    :param memlevel: zlib memory usage level. See zlib documentation for more.
    :param dictionary: zlib compression algorith, see zlib documentation for more.
    :return: Tuple containing (Error code, zlib state object)
    """
    if not state and (src is None or dest is None):
        raise ValueError("No initialised state. Provide src and dest on first call.")

    if state:
        err = Z_OK
    else:
        state = zState()
        err = None

    if src:
        state.next_in = C.cast(C.pointer(src), C.POINTER(C.c_ubyte))
        state.avail_in = len(src)
    elif src == Z_NULL:
        state.next_in = NULL_PTR
        state.avail_in = Z_NULL

    if dest:
        state.next_out = C.cast(C.pointer(dest), C.POINTER(C.c_ubyte))
        state.avail_out = len(dest)

    if err is None:
        err = _zlib.deflateInit2_(C.byref(state), level, Z_DEFLATED, -wbits, memlevel, Z_DEFAULT_STRATEGY, ZLIB_VERSION, SIZEOF_ZSTATE)
        if err == Z_OK and dictionary:
            err = _zlib.deflateSetDictionary(C.byref(state), C.cast(C.c_char_p(dictionary), C.POINTER(C.c_ubyte)), len(dictionary))

    if err == Z_OK:
        err = _zlib.deflate(C.byref(state), mode)

    if err == Z_STREAM_END:
        assert _zlib.deflateEnd(C.byref(state)) == Z_OK

    return err, state


def raw_decompress(src=None, dest=None, mode=Z_FINISH, state=None, wbits=MAX_WBITS, dictionary=None) -> (int, zState):
    """
    Wraps zlib.inflate().
    Manages decompression state and conversion to ctypes compatible types.
    Set src and dest to None to preserve their values between calls.
    :param src: Data buffer to decompress or None.
    :param dest: Data buffer for decompressed data to be written to or None.
    :param mode: Decompression flush mode. Use Z_NO_FLUSH, Z_PARTIAL_FLUSH, or Z_FINISH. See zlib documentation for full description.
    :param state: State instance returned from previous call or None on first call.
    :param wbits: Compression window bit size. Defaults to MAX_WBITS, do not change unless you REALLY know what you are doing.
    :param dictionary: zlib compression algorith, see zlib documentation for more.
    :return: Tuple containing (Error code, zlib state object)
    """
    if not state and (src is None or dest is None):
        raise ValueError("No initialised state. Provide src and dest on first call.")

    if state:
        err = Z_OK
    else:
        state = zState()
        err = Z_NULL

    if src:
        state.next_in = C.cast(C.pointer(src), C.POINTER(C.c_ubyte))
        state.avail_in = len(src)
    elif src == Z_NULL:
        state.next_in = Z_NULL
        state.avail_in = Z_NULL

    if dest:
        state.next_out = C.cast(C.pointer(dest), C.POINTER(C.c_ubyte))
        state.avail_out = len(dest)

    if err == Z_NULL:
        err = _zlib.inflateInit2_(C.byref(state), -wbits, ZLIB_VERSION, SIZEOF_ZSTATE)

    if err == Z_OK:
        err = _zlib.inflate(C.byref(state), mode)
        if err == Z_NEED_DICT:
            assert dictionary, err
            err = _zlib.inflateSetDictionary(C.byref(state), C.cast(C.c_char_p(dictionary), C.POINTER(C.c_ubyte)), len(dictionary))

    if err == Z_STREAM_END:
        _zlib.inflateEnd(C.byref(state))

    return err, state

def bound(state, src_len):
    return _zlib.deflateBound(C.byref(state), src_len)

def crc32(src, crc=None):
    """
    Calculate the CRC32 value of the input data.
    :param src: Buffer containing input data to evaluate.
    :param crc: Existing CRC to add to or None.
    :return: CRC32 value of src.
    """
    return _zlib.crc32(crc or _zlib.crc32(0, Z_NULL, 0), src, len(src))
