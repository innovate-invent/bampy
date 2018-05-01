"""
Provides a convenience iterator to read in block data.
"""

import ctypes as C
import io

from . import zlib
from .block import Block


class EmptyBlock(ValueError):
    """Exception used to signal that a block with no compressed data was found."""
    pass


class _Reader:
    """
    Base class for buffer and stream readers.
    Provides Iterable interface to read in blocks.
    """

    def __init__(self, input):
        """
        Constructor.
        :param input: Block data source.
        """
        self.total_in = 0
        self.total_out = 0
        self.remaining = 0
        self._input = input
        self.buffer = None

    def _inflate(self, block, cdata):
        if self.remaining:
            # Copy forward any remaining data into the next buffer
            edata = (C.c_ubyte * (block.uncompressed_size + self.remaining))()
            C.memmove(edata, C.byref(self.buffer, len(self.buffer) - self.remaining), self.remaining)
            data = (C.c_ubyte * block.uncompressed_size).from_buffer(edata, self.remaining)
        else:
            data = (C.c_ubyte * block.uncompressed_size)()

        cdata = (C.c_ubyte * len(cdata)).from_buffer(cdata)
        res, state = zlib.raw_decompress(cdata, data)
        assert res in (zlib.Z_OK, zlib.Z_STREAM_END), "Invalid zlib data."

        self.total_in += state.total_in
        self.total_out += state.total_out

        if self.remaining:
            self.buffer = edata
            self.remaining += len(data)
        else:
            self.buffer = data
            self.remaining = len(data)

        return self.buffer

    def __iter__(self):
        return self

    def __next__(self):
        raise NotImplementedError()


def Reader(input, offset: int = 0, peek=None) -> _Reader:
    """
    Factory to provide a unified reader interface.
    Resolves if input is randomly accessible and provides the appropriate _Reader implementation.
    :param input: A stream or buffer object.
    :param offset: If input is a buffer, the offset into the buffer to begin reading. Ignored otherwise.
    :param peek: Data consumed from stream while peeking. Will be prepended to read data. Ignored if buffer passed as input.
    :return: An instance of StreamReader or BufferReader.
    """
    if isinstance(input, (io.RawIOBase, io.BufferedIOBase)):
        return StreamReader(input, peek)
    else:
        return BufferReader(input, offset)


class StreamReader(_Reader):
    """
    Implements _Reader to handle input data that is not accessible through a buffer interface.
    """

    def __init__(self, input, peek=None):
        """
        Constructor.
        :param input: Stream object to read from.
        :param peek: Data consumed from stream while peeking. Will be prepended to read data.
        """
        super().__init__(input)
        self._peek = peek

    def __next__(self):
        try:
            block, cdata = Block.from_stream(self._input, self._peek)
            if not block.uncompressed_size:
                raise EmptyBlock()
            self._inflate(block, cdata)
            self._peek = None
            return self.buffer
        except EOFError:
            raise StopIteration()


class BufferReader(_Reader):
    """
    Implements _Reader to handle input data that is accessible through a buffer interface.
    """

    def __init__(self, input, offset=0):
        """
        Constructor.
        :param input: Buffer object to read from.
        :param offset: The offset into the input buffer to begin reading from.
        """
        super().__init__(input)
        self._len = len(input)
        self.offset = offset

    def __next__(self):
        if self.offset < self._len:
            block, cdata = Block.from_buffer(self._input, self.offset)
            self.offset += len(block)
            if block.uncompressed_size:
                return self._inflate(block, cdata)
            else:
                raise EmptyBlock()
        raise StopIteration()
