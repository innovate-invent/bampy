import ctypes as C

MAGIC = b'\x1F\x8B'

MAX_BLOCK_SIZE = 2 ** 16 - 1

MAX_BLOCK_BUFFER_TYPE = C.c_ubyte * MAX_BLOCK_SIZE

EMPTY_BLOCK = b'\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00\x42\x43\x02\x00\x1b\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00'

def is_bgzf(buffer, offset = 0):
    return buffer[offset:offset+2] == MAGIC

class InvalidBGZF(ValueError):
    pass