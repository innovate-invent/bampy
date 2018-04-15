import warnings
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from .. import bam, bgzf
from ..reader import BAMStreamReader, SAMStreamReader, BAMBufferReader, SAMBufferReader, BGZFReader as _BGZFReader, TruncatedFileWarning
from .bgzf import Reader as bgzf_Reader

import io, ctypes as C

_Last = namedtuple('buffer', 'offset', 'remaining')


class BGZFReader(_BGZFReader):
    def __init__(self, input, offset=0, peek=None, threadpool: ThreadPoolExecutor = ThreadPoolExecutor(thread_name_prefix='BGZF_WORKER')):
        self.pool = threadpool
        super().__init__(input, offset)
        self._last = _Last(self._bgzfReader.buffer, self._bgzfOffset, self._bgzfReader.remaining)
        # Re-init with multithreaded reader
        self._bgzfReader = bgzf_Reader(self._input, self._bgzfReader.offset if hasattr(self._bgzfReader, 'offset') else 0)

    def __next__(self):
        empty = False
        while True:
            if self._last.remaining and not empty:
                # Move record into contiguous memory
                record_len = C.c_uint32.from_buffer(self._last.buffer, self._last.offset).value
                offset = record_len - self._last.remaining
                buffer = (C.c_ubyte * record_len)()
                C.memmove(buffer, C.byref(self._last.buffer, self._last.offset), self._last.remaining)
                C.memmove(buffer, C.byref(self._bgzfReader.buffer), offset)
                self._bgzfOffset = offset
                self._bgzfReader.remaining -= offset
                yield bam.Record.from_buffer(buffer, 0, self.references)
            try:
                while True:
                    record = bam.Record.from_buffer(self._bgzfReader.buffer, self._bgzfOffset, self.references)
                    record_len = len(record)
                    self._bgzfOffset += record_len
                    self._bgzfReader.remaining -= record_len
                    yield record
            except bam.util.BufferUnderflow:
                try:
                    self._last = _Last(self._bgzfReader.buffer, self._bgzfOffset, self._bgzfReader.remaining)
                    next(self._bgzfReader)
                    empty = False
                except bgzf.EmptyBlock:
                    empty = True
                except StopIteration:
                    if empty:
                        warnings.warn("Missing EOF marker, data is possibly truncated.", TruncatedFileWarning)
                    raise
                self._bgzfOffset = 0


def Reader(input, offset=0, threadpool: ThreadPoolExecutor = ThreadPoolExecutor(thread_name_prefix='BAMPY_WORKER')):
    """
    Convenience interface for reading alignment records from BGZF/BAM/SAM files.
    :param input: Stream or buffer containing alignment data.
    :param offset: If input is a buffer, offset into buffer to begin reading from.
    :return: Iterable that emits Record instances.
    """
    if isinstance(input, (io.RawIOBase, io.BufferedIOBase)):
        # Stream
        peek = bytearray(4)
        input.readinto(peek)
        if bgzf.is_bgzf(peek):
            return BGZFReader(input, offset, peek, threadpool)
        elif bam.is_bam(peek):
            return BAMStreamReader(input, peek)
        else:
            # SAM
            return SAMStreamReader(input, peek)
    else:
        if bgzf.is_bgzf(input, offset):
            return BGZFReader(input, offset, threadpool=threadpool)
        elif bam.is_bam(input, offset):
            return BAMBufferReader(input, offset)
        else:
            # SAM
            return SAMBufferReader(input, offset) #TODO multithread sam parsing, eeeww