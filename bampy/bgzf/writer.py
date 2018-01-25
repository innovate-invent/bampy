from . import block, zlib
from .block import MAX_CDATA_SIZE
from .util import MAX_BLOCK_SIZE
import ctypes as C
import io

SIZEOF_TRAILER = C.sizeof(block.Trailer)

SIZEOF_FixedXLENHeader = C.sizeof(block.FixedXLENHeader)


def Writer(output, offset=0):
    if isinstance(output, (io.RawIOBase, io.BufferedIOBase)):
        return StreamWriter(output)
    else:
        return BufferWriter(output, offset)

class _Writer:
    def __init__(self, output, offset=0):
        self._state = None
        self._header = None
        self.total_in = 0
        self.total_out = 0
        self.offset = offset
        self._output = output

    def _deflate(self, data):
        state = self._state
        if not state:
            self._header = block.FixedXLENHeader.from_buffer(self._data_buffer, self.offset)
            self.offset += SIZEOF_FixedXLENHeader
            self._cdata = (C.c_ubyte * min(MAX_CDATA_SIZE, len(self._data_buffer)-self.offset)).from_buffer(self._data_buffer, self.offset)

        data_len = len(data)

        if data_len > MAX_CDATA_SIZE:
            # Must split data so fill remainder
            res, state = zlib.raw_compress(data, mode=zlib.Z_FINISH, state=state)
            self._state = state
            self.finish_block(False)
            return data_len - state.avail_in
        elif state and state.total_out + data_len > MAX_CDATA_SIZE:
            self.finish_block()
            return 0
        else:
            res, self._state = zlib.raw_compress(data, None if state else self._cdata, mode=zlib.Z_NO_FLUSH, state=state)
            return data_len

    def finish_block(self, flush = True):
        if not self._header: return
        state = self._state
        if flush and state:
            res, state = zlib.raw_compress(zlib.Z_NULL, mode=zlib.Z_FINISH, state=state)
            assert res == zlib.Z_STREAM_END, "Failed to flush buffer."
        self._header.BSIZE.value = state.total_out - 1  # This heavily relies on the random access ability of the provided buffer
        self.offset += state.total_out
        trailer = block.Trailer.from_buffer(self._data_buffer, self.offset)
        trailer.crc32 = state.adler
        trailer.uncompressed_size = state.total_in
        self.offset += SIZEOF_TRAILER
        self._state = None
        self._header = None

    def block_remaining(self):
        return MAX_CDATA_SIZE - self._state.total_out if self._state else 0

    def __call__(self, data):
        raise NotImplementedError()

    def __del__(self):
        if self._state:
            self.finish_block(True)


class BufferWriter(_Writer):
    def __init__(self, output, offset=0):
        super().__init__(output, offset)
        self._data_buffer = output

    def __call__(self, data):
        data_len = len(data)
        data_offset = 0
        while data_offset < data_len:
            data_offset = self._deflate((C.c_ubyte * (data_len-data_offset)).from_buffer(data, data_offset))


class StreamWriter(_Writer):
    def __init__(self, output):
        super().__init__(output, 0)
        self._data_buffer = bytearray(MAX_BLOCK_SIZE)

    def __call__(self, data):
        data_len = len(data)
        offset = 0
        while offset < data_len:
            offset = self._deflate((C.c_ubyte * (data_len-offset)).from_buffer(data, offset))
            self._output.write(self._data_buffer[:self.offset])
            self.offset = 0