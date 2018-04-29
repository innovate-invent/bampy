import io
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import numba

from bampy.mt import CACHE_JIT
from . import zlib
from ...bgzf import Block
from ...bgzf.reader import BufferReader, EmptyBlock, StreamReader, _Reader as __Reader


@numba.jit(nopython=True, nogil=True, cache=CACHE_JIT)
def inflate(data, buffer, offset=0):
    zlib.raw_decompress(data, buffer[offset:])
    return buffer, offset


class _Reader(__Reader):
    """
    Base class for buffer and stream readers.
    Provides Iterable interface to read in blocks.
    """

    def __init__(self, input, threadpool: ThreadPoolExecutor):
        """
        Constructor.
        :param input: Block data source.
        """
        super().__init__(input)
        self.pool = threadpool
        self.blockqueue = deque()
        self.max_queued = 0

    # def _init_buffer(self, size):
    #     buffer = (C.c_ubyte * (size + self.remaining))()
    #     self.buffer = buffer
    #     if self.remaining:
    #         # Copy forward any remaining data into the next buffer
    #         C.memmove(buffer, C.byref(self.buffer, len(self.buffer) - self.remaining), self.remaining)
    #         buffer = (C.c_ubyte * size).from_buffer(buffer, self.remaining)
    #     self.remaining += size
    #     return buffer

    @property
    def buffer(self):
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
        if not self.max_queued or not self.blockqueue[0].done(): self.max_queued += 1
        try:
            while len(self.blockqueue) < self.max_queued:
                block, cdata = Block.from_stream(self._input, self._peek)
                self._peek = None
                self.total_in += len(block)
                self.total_out += block.uncompressed_size
                if block.uncompressed_size:
                    self.blockqueue.append(self.pool.submit(inflate, cdata, bytearray(block.uncompressed_size)))  # TODO reuse buffers
                else:
                    raise EmptyBlock()
        except EOFError:
            pass
        if not len(self.blockqueue):
            raise StopIteration()
        self.buffer = self.blockqueue.popleft().result()
        self.remaining = len(self.buffer)
        return self.buffer


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
        if not self.max_queued or not self.blockqueue[0].done(): self.max_queued += 1
        while self.offset < self._len and len(self.blockqueue) < self.max_queued:
            block, cdata = Block.from_buffer(self._input, self.offset)
            block_len = len(block)
            self.offset += block_len
            self.total_in += block_len
            self.total_out += block.uncompressed_size
            if block.uncompressed_size:
                self.blockqueue.append(self.pool.submit(inflate, cdata, bytearray(block.uncompressed_size)))  # TODO reuse buffers
            else:
                raise EmptyBlock()
        if not len(self.blockqueue):
            raise StopIteration()
        self.buffer = self.blockqueue.popleft().result()
        self.remaining = len(self.buffer)
        return self.buffer
