from collections import deque
from concurrent.futures import ThreadPoolExecutor
import ctypes as C, io, numba

from . import zlib

from bampy.bgzf.writer import SIZEOF_TRAILER, SIZEOF_FIXED_XLEN_HEADER, SIZEOF_UINT16, MAX_CDATA_SIZE, MAX_BLOCK_SIZE
from bampy.bgzf.block import FIXED_XLEN_HEADER, Trailer


@numba.jit(nopython=True, nogil=True)
def deflate(data, buffer, offset=0):
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
            res, state = zlib.raw_compress(curr, None if state else buffer, mode=zlib.Z_NO_FLUSH, state=state)
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
    def __init__(self, output, thread_pool: ThreadPoolExecutor):
        self.pool = thread_pool
        self.output = output
        self.queue = deque()
        self.queue_size = 0
        self.results = deque()

    def __call__(self, data):
        self.queue.append(data)
        self.queue_size += len(data)
        if self.queue_size >= MAX_CDATA_SIZE: #TODO calculate actual max
            self.results.append(self.pool.submit(deflate, self.queue, bytearray(MAX_BLOCK_SIZE))) #TODO reuse buffers
            self.queue = deque()
            self.queue_size = 0
            self.flush()

    def flush(self, wait=False):
        raise NotImplementedError()

    def __del__(self):
        self.flush(True)


class BufferWriter(_Writer):
    def __init__(self, output, offset=0, thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(thread_name_prefix='BGZF_WORKER')):
        self.offset = offset
        super().__init__(output, thread_pool)

    def flush(self, wait=False):
        while self.results[0].done() or wait:
            buffer, offset = self.results.popleft().result()
            self.output[self.offset:self.offset+offset] = buffer[:offset]
            self.offset += offset


class StreamWriter(_Writer):
    def flush(self, wait=False):
        while self.results[0].done() or wait:
            buffer, offset = self.results.popleft().result()
            self.output.write(buffer[:offset])


def Writer(output, offset=0, thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(thread_name_prefix='BGZF_WORKER')) -> _Writer:
    """
    Factory to provide a unified writer interface.
    Resolves if output is randomly accessible and provides the appropriate _Writer implementation.
    :param output: A stream or buffer object.
    :param offset: If output is a buffer, the offset into the buffer to begin writing. Ignored otherwise.
    :return: An instance of StreamWriter or BufferWriter.
    """
    if isinstance(output, (io.RawIOBase, io.BufferedIOBase)):
        return StreamWriter(output, thread_pool)
    else:
        return BufferWriter(output, offset, thread_pool)

