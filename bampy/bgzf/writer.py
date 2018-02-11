from . import block, zlib
from .block import MAX_CDATA_SIZE
from .util import MAX_BLOCK_SIZE
import ctypes as C
import io

SIZEOF_TRAILER = C.sizeof(block.Trailer)

SIZEOF_FIXED_XLEN_HEADER = len(block.FIXED_XLEN_HEADER)

SIZEOF_UINT16 = C.sizeof(C.c_uint16)


def Writer(output, offset=0):
    if isinstance(output, (io.RawIOBase, io.BufferedIOBase)):
        return StreamWriter(output)
    else:
        return BufferWriter(output, offset)

class _Writer:
    def __init__(self, output, offset=0):
        self._state = None
        self._bsize = None
        self.total_in = 0
        self.total_out = 0
        self.offset = offset
        self._output = output

    def _deflate(self, data, boundary=False) -> int:
        """
        Manages the deflation state ensuring it does not exceed the block maximum size.
        :param data: The data to be compressed
        :param boundary: Denotes a data boundary to partially flush the compression buffer and update the value block_remaining() returns
        :return: Offset into data that has been processed
        """
        state = self._state
        if not state:
            self._data_buffer[self.offset:self.offset + SIZEOF_FIXED_XLEN_HEADER] = block.FIXED_XLEN_HEADER
            self.offset += SIZEOF_FIXED_XLEN_HEADER
            self._bsize = C.c_uint16.from_buffer(self._data_buffer, self.offset)
            self._bsize.value = SIZEOF_FIXED_XLEN_HEADER + SIZEOF_TRAILER + 1
            self.offset += SIZEOF_UINT16

            self._cdata_len = min(MAX_CDATA_SIZE, len(self._data_buffer)-self.offset)
            self._cdata = (C.c_ubyte * self._cdata_len).from_buffer(self._data_buffer, self.offset)

        data_len = len(data)

        if data_len > self._cdata_len:
            # Must split data so fill remainder
            res, state = zlib.raw_compress(data, mode=zlib.Z_FINISH, state=state)
            assert res == zlib.Z_STREAM_END
            self._state = state
            self.finish_block(False)
            return data_len - state.avail_in
        elif state and state.total_out + data_len > self._cdata_len:
            self.finish_block()
            return 0
        else:
            res, self._state = zlib.raw_compress(data, None if state else self._cdata, mode=zlib.Z_PARTIAL_FLUSH if boundary else zlib.Z_NO_FLUSH, state=state)
            assert res == zlib.Z_OK
            return data_len

    def finish_block(self, flush = True):
        if self._bsize is None: return
        state = self._state
        if flush and state:
            res, state = zlib.raw_compress(zlib.Z_NULL, mode=zlib.Z_FINISH, state=state)
            assert res == zlib.Z_STREAM_END, "Failed to flush buffer."
        if state:
            self._bsize.value += state.total_out # This heavily relies on the random access ability of the provided buffer
            self.offset += state.total_out
        trailer = block.Trailer.from_buffer(self._data_buffer, self.offset)
        trailer.crc32 = state.adler
        trailer.uncompressed_size = state.total_in
        self.offset += SIZEOF_TRAILER
        self._state = None
        self._bsize = None

    def block_remaining(self):
        return MAX_CDATA_SIZE - self._state.total_out if self._state else 0

    def __call__(self, data, boundary=False):
        data_len = C.sizeof(data) if isinstance(data, C.Array) else len(data)
        data_offset = 0
        while data_offset < data_len:
            data_offset = self._deflate((C.c_ubyte * (data_len-data_offset)).from_buffer(data, data_offset), boundary=boundary)

    def __del__(self):
        if self._state:
            self.finish_block(True)


class BufferWriter(_Writer):
    def __init__(self, output, offset=0):
        super().__init__(output, offset)
        self._data_buffer = output


class StreamWriter(_Writer):
    def __init__(self, output):
        super().__init__(output, 0)
        self._data_buffer = bytearray(MAX_BLOCK_SIZE)

    def finish_block(self, flush = True):
        if not self.offset: return
        super().finish_block(flush)
        self._output.write(self._data_buffer[:self.offset])
        self.offset = 0