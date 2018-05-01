import ctypes as C
import io
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import numba

from bampy.bgzf.block import FIXED_XLEN_HEADER, Trailer
from bampy.bgzf.writer import MAX_BLOCK_SIZE, MAX_DATA_SIZE, SIZEOF_FIXED_XLEN_HEADER, SIZEOF_TRAILER, SIZEOF_UINT16
from bampy.mt import CACHE_JIT, THREAD_NAME, DEFAULT_THREADS
from . import zlib


@numba.jit(nopython=True, nogil=True, cache=CACHE_JIT)
def deflate(data, buffer, offset=0, level=zlib.DEFAULT_COMPRESSION_LEVEL):
    buffer[offset:offset + SIZEOF_FIXED_XLEN_HEADER] = FIXED_XLEN_HEADER
    offset += SIZEOF_FIXED_XLEN_HEADER
    bsize = C.c_uint16.from_buffer(buffer, offset)
    bsize.value = SIZEOF_FIXED_XLEN_HEADER + SIZEOF_TRAILER + 1
    offset += SIZEOF_UINT16
    state = None

    i = iter(data)
    curr = next(i)
    nxt = next(i)
    while True:
        try:
            res, state = zlib.raw_compress(curr, None if state else buffer, mode=zlib.Z_NO_FLUSH, state=state, level=level)
            assert res == zlib.Z_OK
            curr = nxt
            nxt = next(i)
        except StopIteration:
            res, state = zlib.raw_compress(curr, mode=zlib.Z_FINISH, state=state)
            assert res == zlib.Z_STREAM_END, "Failed to flush buffer (Code: {}).".format(res)
            bsize.value += state.total_out
            offset += state.total_out
            trailer = Trailer.from_buffer(buffer, offset)
            offset += SIZEOF_TRAILER
            trailer.crc32 = state.adler
            trailer.uncompressed_size = state.total_in
            return buffer, offset


class _Writer:
    def __init__(self, output, thread_pool: ThreadPoolExecutor, _thread_func=deflate, level=zlib.DEFAULT_COMPRESSION_LEVEL):
        self.pool = thread_pool
        self._thread_func = _thread_func
        self.output = output
        self.queue = deque()
        self.queue_size = 0
        self.results = deque()
        self._level = level

    def __call__(self, data):
        size = len(data)
        if self.queue_size + size >= MAX_DATA_SIZE:
            self.finish_block()
        self.queue.append(data)
        self.queue_size += size

    def finish_block(self):
        if self.queue_size:
            self.results.append(self.pool.submit(self._thread_func, self.queue, bytearray(MAX_BLOCK_SIZE)), self._level)  # TODO reuse buffers
            self.queue = deque()  # TODO reuse queues
            self.queue_size = 0
            self.flush()

    def flush(self, wait=False):
        raise NotImplementedError()

    def __del__(self):
        self.finish_block()
        self.flush(True)


class BufferWriter(_Writer):
    def __init__(self, output, offset=0, thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=DEFAULT_THREADS, thread_name_prefix=THREAD_NAME), level=zlib.DEFAULT_COMPRESSION_LEVEL):
        self.offset = offset
        super().__init__(output, thread_pool, level=level)

    def flush(self, wait=False):
        while self.results[0].done() or wait:
            buffer, offset = self.results.popleft().result()
            self.output[self.offset:self.offset + offset] = buffer[:offset]
            self.offset += offset


class StreamWriter(_Writer):
    def flush(self, wait=False):
        while self.results[0].done() or wait:
            buffer, offset = self.results.popleft().result()
            self.output.write(buffer[:offset])


def Writer(output, offset=0, thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=DEFAULT_THREADS, thread_name_prefix=THREAD_NAME), level=zlib.DEFAULT_COMPRESSION_LEVEL) -> _Writer:
    """
    Factory to provide a unified writer interface.
    Resolves if output is randomly accessible and provides the appropriate _Writer implementation.
    :param output: A stream or buffer object.
    :param offset: If output is a buffer, the offset into the buffer to begin writing. Ignored otherwise.
    :return: An instance of StreamWriter or BufferWriter.
    """
    if isinstance(output, (io.RawIOBase, io.BufferedIOBase)):
        return StreamWriter(output, thread_pool, level=level)
    else:
        return BufferWriter(output, offset, thread_pool, level=level)
