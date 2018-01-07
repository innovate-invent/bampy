from bampy.bgzf import Block
from bampy.zlib import raw_decompress
from bampy.bam import header_from_buffer
from bampy import Record
import ctypes

f = open("normal.bam", 'rb')
header = None
try:
    while True:
        b, d = Block.fromStream(f)

        data = (ctypes.c_ubyte * b.uncompressed_size)()
        cdata = (ctypes.c_ubyte * len(d)).from_buffer(d)
        res, _ = raw_decompress(cdata, data)
        offset = 0
        if not header:
            header, refs, offset = header_from_buffer(data)

        while offset < len(data):
            r = Record.fromBuffer(data, offset, refs)
            offset += r.block_size
except EOFError:
    pass