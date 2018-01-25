import ctypes as C

MAGIC = b'\x1F\x8B'

MAX_BLOCK_SIZE = 2 ** 16 - 1

MAX_BLOCK_BUFFER_TYPE = C.c_ubyte * MAX_BLOCK_SIZE

def is_bgzf(buffer, offset = 0):
    return buffer[offset:offset+2] == MAGIC

class InvalidBGZF(ValueError):
    pass