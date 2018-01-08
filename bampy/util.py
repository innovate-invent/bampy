from .bgzf import Block, isBGZF
from .zlib import raw_decompress
from .bam import Record, header_from_buffer, header_from_stream, isBAM
import ctypes as C

def discoverStream(stream):
    peek = bytearray(4)
    stream.readinto(peek)
    if isBGZF(peek):
        return Block.from_stream(stream, peek)
    elif isBAM(peek):
        return header_from_stream(stream, peek)
    else:
        #SAM
        return #TODO

def streamReader_BGZF(stream, header=None):
    remaining = 0
    try:
        while True:
            block, cdata = Block.from_stream(stream)
            cdata = (C.c_ubyte * len(cdata)).from_buffer(cdata)
            if remaining:
                edata = (C.c_ubyte * (block.uncompressed_size + remaining))()
                C.memmove(edata, C.byref(data, offset), remaining)
                data = (C.c_ubyte * block.uncompressed_size).from_buffer(edata, remaining)
            else:
                data = (C.c_ubyte * block.uncompressed_size)()

            res, _ = raw_decompress(cdata, data)
            offset = 0

            if remaining:
                data = edata

            if not header:
                header, refs, offset = header_from_buffer(data)

            remaining = len(data) - offset
            if remaining > C.sizeof(C.c_uint32):
                record_size = C.c_int32.from_buffer(data, offset).value + C.sizeof(C.c_uint32)
                while record_size < remaining:
                    record = Record.fromBuffer(data, offset, refs)
                    yield record
                    offset += len(record)
                    remaining = len(data) - offset
                    if remaining > C.sizeof(C.c_uint32):
                        record_size = C.c_uint32.from_buffer(data, offset).value + C.sizeof(C.c_uint32)
                    else:
                        break
    except EOFError:
        pass