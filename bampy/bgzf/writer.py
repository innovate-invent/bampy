"""
Provides convenience interface to write data to BGZF blocks.
"""

import ctypes as C
import io

from . import block, zlib
from .block import MAX_CDATA_SIZE, MAX_DATA_SIZE
from .util import MAX_BLOCK_SIZE

SIZEOF_TRAILER = C.sizeof(block.Trailer)

SIZEOF_FIXED_XLEN_HEADER = len(block.FIXED_XLEN_HEADER)

SIZEOF_UINT16 = C.sizeof(C.c_uint16)


class _Writer:
    """
    Base class for buffer and stream writers.
    Provides Callable interface to compress data into blocks.
    """
    def __init__(self, output, offset=0):
        """
        Constructor.
        :param output: The buffer to output compressed data.
        :param offset: The offset into buffer to begin writing.
        """
        self._state = None
        self._bsize = None
        self.total_in = 0
        self.total_out = 0
        self.offset = offset
        self._output = output

    def _deflate(self, data) -> int:
        """
        Manages the deflation state ensuring it does not exceed the block maximum size.
        :param data: The data to be compressed
        :return: Offset into data that has been processed
        """
        state = self._state
        if not state:
            self._data_buffer[self.offset:self.offset + SIZEOF_FIXED_XLEN_HEADER] = block.FIXED_XLEN_HEADER
            self.offset += SIZEOF_FIXED_XLEN_HEADER
            self._bsize = C.c_uint16.from_buffer(self._data_buffer, self.offset)
            self._bsize.value = SIZEOF_FIXED_XLEN_HEADER + SIZEOF_TRAILER + 1
            self.offset += SIZEOF_UINT16

            self._cdata_len = min(MAX_CDATA_SIZE, len(self._data_buffer) - self.offset)
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
            # Not enough space left in block so finalize
            self.finish_block()
            return 0
        else:
            res, self._state = zlib.raw_compress(data, None if state else self._cdata, mode=zlib.Z_NO_FLUSH, state=state)
            assert res == zlib.Z_OK
            return data_len

    def finish_block(self, flush=True):
        """
        Finalises current BGZF block.
        :param flush: Set to false if Z_FINISH already passed to zlib.raw_compress() for current state.
        :return: None
        """
        if self._bsize is None: return
        state = self._state
        if flush and state:
            res, state = zlib.raw_compress(zlib.Z_NULL, mode=zlib.Z_FINISH, state=state)
            assert res == zlib.Z_STREAM_END, "Failed to flush buffer (Code: {}).".format(res)
        if state:
            self._bsize.value += state.total_out  # This heavily relies on the random access ability of the provided buffer
            self.offset += state.total_out
        trailer = block.Trailer.from_buffer(self._data_buffer, self.offset)
        trailer.crc32 = state.adler
        trailer.uncompressed_size = state.total_in
        self.offset += SIZEOF_TRAILER
        self._state = None
        self._bsize = None

    def block_remaining(self) -> int:
        """
        Calculates amount of remaining space in current block after compression.
        Is only accurate after after setting boundary=True when calling __call__().
        :return: Amount of remaining space in bytes.
        """
        return (MAX_CDATA_SIZE - self._state.total_out - 1) if self._state else 0

    def __call__(self, data):
        """
        Pass data to the zlib compressor.
        Setting boundary to True flushes the compression buffer. Doing this often can severely impact compression efficacy.
        Passing data larger than the maximum block size will result in the data being split between blocks.
        A new block may be created between calls. To gaurantee that data is compressed into the same block check block_remaining()
        before submitting the data.
        :param data: Data to add to compression stream.
        :return: None
        """
        data_len = C.sizeof(data) if isinstance(data, C.Array) else len(data)
        data_offset = 0
        while data_offset < data_len:
            data_offset = self._deflate((C.c_ubyte * (data_len - data_offset)).from_buffer(data, data_offset))

    def __del__(self):
        if self._state:
            self.finish_block(True)


def Writer(output, offset=0) -> _Writer:
    """
    Factory to provide a unified writer interface.
    Resolves if output is randomly accessible and provides the appropriate _Writer implementation.
    :param output: A stream or buffer object.
    :param offset: If output is a buffer, the offset into the buffer to begin writing. Ignored otherwise.
    :return: An instance of StreamWriter or BufferWriter.
    """
    if isinstance(output, (io.RawIOBase, io.BufferedIOBase)):
        return StreamWriter(output)
    else:
        return BufferWriter(output, offset)


class BufferWriter(_Writer):
    """
    Implements _Writer to output to a randomly accessible buffer interface.
    """
    def __init__(self, output, offset=0):
        super().__init__(output, offset)
        self._data_buffer = output


class StreamWriter(_Writer):
    """
    Implements _Writer to output to a stream.
    Internally buffers each block until it is finished before writing to the stream.
    """
    def __init__(self, output):
        super().__init__(output, 0)
        self._data_buffer = bytearray(MAX_BLOCK_SIZE)

    def finish_block(self, flush=True):
        if self.offset:
            super().finish_block(flush)
            self._output.write(self._data_buffer[:self.offset])
            self.offset = 0
