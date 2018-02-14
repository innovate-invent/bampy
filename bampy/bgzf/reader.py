import ctypes as C
import io

from . import zlib
from .block import Block


class EmptyBlock(ValueError):
    pass


def Reader(input, offset: int = 0, peek=None):
    if isinstance(input, (io.RawIOBase, io.BufferedIOBase)):
        return StreamReader(input, peek)
    else:
        return BufferReader(input, offset)


class _Reader:
    def __init__(self, input):
        self.total_in = 0
        self.total_out = 0
        self.remaining = 0
        self._input = input

    def _inflate(self, block, cdata):
        if self.remaining:
            edata = (C.c_ubyte * (block.uncompressed_size + self.remaining))()
            C.memmove(edata, C.byref(self._data, len(self._data) - self.remaining), self.remaining)
            data = (C.c_ubyte * block.uncompressed_size).from_buffer(edata, self.remaining)
        else:
            data = (C.c_ubyte * block.uncompressed_size)()

        cdata = (C.c_ubyte * len(cdata)).from_buffer(cdata)
        res, state = zlib.raw_decompress(cdata, data)
        assert res in (zlib.Z_OK, zlib.Z_STREAM_END), "Invalid zlib data."

        self.total_in += state.total_in
        self.total_out += state.total_out

        if self.remaining:
            self._data = edata
            self.remaining += len(data)
        else:
            self._data = data
            self.remaining = len(data)

        return self._data

    @property
    def buffer(self):
        return self._data

    def __iter__(self):
        return self

    def __next__(self):
        raise NotImplementedError()


class StreamReader(_Reader):
    def __init__(self, input, peek=None):
        super().__init__(input)
        self._peek = peek

    def __next__(self):
        try:
            block, cdata = Block.from_stream(self._input, self._peek)
            if not block.uncompressed_size:
                raise EmptyBlock()
            self._inflate(block, cdata)
            self._peek = None
            return self._data
        except EOFError:
            raise StopIteration()


class BufferReader(_Reader):
    def __init__(self, input, offset=0):
        super().__init__(input)
        self._len = len(input)
        self.offset = offset

    def __next__(self):
        while self.offset < self._len:
            block, cdata = Block.from_buffer(self._input, self.offset)
            self.offset += len(block)
            if block.uncompressed_size:
                return self._inflate(block, cdata)
            else:
                raise EmptyBlock()
        raise StopIteration()
