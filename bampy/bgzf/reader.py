from . import zlib
from .block import Block
import ctypes as C

class Reader:
    def __init__(self, input, offset:int = 0, peek = None):
        self.total_in = 0
        self.total_out = 0
        self.remaining = 0
        if isinstance(input, 'RawIOBase'):
            self._peek = peek
            self.__next__ = self._stream
        else:
            self.__next__ = self._buffer
            self._len = len(input)
            self.offset = offset
        self._input = input

    def _stream(self):
        try:
            return self._inflate(*Block.from_stream(self._input, self._peek))
        except EOFError:
            raise StopIteration()

    def _buffer(self):
        if self.offset < self._len:
            block, cdata = Block.from_buffer(self._input, self.offset)
            self.offset += len(block)
            return self._inflate(block, cdata)
        raise StopIteration()

    def _inflate(self, block, cdata):
        if self.remaining:
            edata = (C.c_ubyte * (block.uncompressed_size + self.remaining))()
            C.memmove(edata, C.byref(self._data, len(self._data) - self.remaining), self.remaining)
            data = (C.c_ubyte * block.uncompressed_size).from_buffer(edata, self.remaining)
        else:
            data = (C.c_ubyte * block.uncompressed_size)()

        cdata = (C.c_ubyte * len(cdata)).from_buffer(cdata)
        res, state = zlib.raw_decompress(cdata, data)
        assert res == zlib.Z_OK, "Invalid zlib data."

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