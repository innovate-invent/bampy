from .bgzf import Block, isBGZF, FixedXLENHeader, Trailer, MAX_BLOCK_SIZE
import bampy.zlib as zlib
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

def _streamBGZFGrabber(stream):
    while True:
        yield Block.from_stream(stream)

def _bufferBGZFGrabber(buffer, offset = 0):
    while offset < len(buffer):
        block, cdata = Block.from_buffer(buffer, offset)
        offset += len(block)
        yield block, cdata
    raise EOFError()

def _readBGZF(grabber, header = None):
    remaining = 0
    try:
        while True:
            block, cdata = grabber()
            cdata = (C.c_ubyte * len(cdata)).from_buffer(cdata)
            if remaining:
                edata = (C.c_ubyte * (block.uncompressed_size + remaining))()
                C.memmove(edata, C.byref(data, offset), remaining)
                data = (C.c_ubyte * block.uncompressed_size).from_buffer(edata, remaining)
            else:
                data = (C.c_ubyte * block.uncompressed_size)()

            res, _ = zlib.raw_decompress(cdata, data)
            offset = 0

            if remaining:
                data = edata

            if not header:
                header, refs, offset = header_from_buffer(data)

            remaining = len(data) - offset
            if remaining > C.sizeof(C.c_uint32):
                record_size = C.c_int32.from_buffer(data, offset).value + C.sizeof(C.c_uint32)
                while record_size < remaining:
                    record = Record.from_buffer(data, offset, refs)
                    yield record
                    offset += len(record)
                    remaining = len(data) - offset
                    if remaining > C.sizeof(C.c_uint32):
                        record_size = C.c_uint32.from_buffer(data, offset).value + C.sizeof(C.c_uint32)
                    else:
                        break
    except EOFError:
        pass

def bufferReader_BGZF(buffer, offset = 0, header = None):
    return _readBGZF(_bufferBGZFGrabber(buffer, offset), header)

def streamReader_BGZF(stream, header=None):
    return _readBGZF(_streamBGZFGrabber(stream), header)

def _writeBGZF(records, buffer, offset = 0, stream=None):
    total_in, total_out = 0, 0
    state = None
    for record in records:  # type: Record
        # Check if block full and finalize
        state, offset, t_in, t_out = _finishBufferedBlock(buffer, offset, state, header, record)
        total_in += t_in
        total_out += t_out

        if not state:
            if stream:
                if buffer:
                    stream.write(buffer)
                offset = 0
            header = FixedXLENHeader.from_buffer(buffer, offset)
            offset += C.sizeof(FixedXLENHeader)
            cdata = (C.c_ubyte * (MAX_BLOCK_SIZE - C.sizeof(FixedXLENHeader) - C.sizeof(Trailer))).from_buffer(buffer,
                                                                                                               offset)

        record.pack()
        res, state = zlib.raw_compress(record._header, cdata, method=zlib.Z_NO_FLUSH, state=state)
        res, state = zlib.raw_compress(record.name, method=zlib.Z_NO_FLUSH, state=state)
        res, state = zlib.raw_compress(record.cigar, method=zlib.Z_NO_FLUSH, state=state)
        res, state = zlib.raw_compress(record.sequence, method=zlib.Z_NO_FLUSH, state=state)
        res, state = zlib.raw_compress(record.quality_scores, method=zlib.Z_NO_FLUSH, state=state)
        for tag in record.tags.values():
            res, state = zlib.raw_compress(tag._header, method=zlib.Z_NO_FLUSH, state=state)
            res, state = zlib.raw_compress(tag._buffer, method=zlib.Z_NO_FLUSH, state=state)

        res, state = zlib.raw_compress(method=zlib.Z_PARTIAL_FLUSH, state=state)  # Ensure state.avail_out is accurate

    state, offset, t_in, t_out = _finishBufferedBlock(buffer, offset, state, header)
    total_in += t_in
    total_out += t_out

    if stream:
        if buffer:
            stream.write(buffer)

    return total_in, total_out

def _finishBufferedBlock(buffer, offset, state, header, record=None):
    if state and (record is None or state.avail_out < len(record)):
        # If cant fit next record in block size limit, finalise block
        zlib.raw_compress(zlib.Z_NULL, method=zlib.Z_FINISH, state=state)  # Flush last of state buffer
        header.BSIZE.value = state.total_out - 1  # This heavily relies on the random access ability of the provided buffer
        offset += state.total_out
        trailer = Trailer.from_buffer(buffer, offset)
        trailer.crc32 = state.adler
        trailer.uncompressed_size = state.total_in
        offset += C.sizeof(Trailer)
        return None, offset, state.total_in, state.total_out
    return state, offset, 0, 0

def bufferWriter_BGZF(records, buffer, offset = 0):
    return _writeBGZF(records, buffer, offset)

def streamWriter_BGZF(stream, records):
    return _writeBGZF(records, bytearray(MAX_BLOCK_SIZE), stream=stream)

