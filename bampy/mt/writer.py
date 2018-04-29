import io
from concurrent.futures import ThreadPoolExecutor
from queue import deque

import numba

from bampy.mt import CACHE_JIT, THREAD_NAME
from . import bgzf
from .bgzf.writer import deflate
from .. import bam
from ..writer import BGZFWriter as _BGZFWriter, Writer as _Writer


@numba.jit(nopython=True, nogil=True, cache=CACHE_JIT)
def compile(queue, buffer, offset=0):
    data_queue = deque()
    for record in queue:
        data = record.pack()
        data_queue.extend(data)
    return deflate(data_queue, buffer, offset)


class BGZFWriter(_BGZFWriter):
    def __init__(self, output, offset=0, threadpool: ThreadPoolExecutor = ThreadPoolExecutor(thread_name_prefix=THREAD_NAME)):
        super().__init__(bgzf.Writer(output, offset, threadpool, compile))

    def __call__(self, record):
        self._output(record)

    def finalize(self):
        if self._output:
            self._output.flush(True)
            offset = self._output.offset
            output = self._output._output
            if isinstance(output, (io.RawIOBase, io.BufferedIOBase)):
                output.write(bgzf.EMPTY_BLOCK)
            else:
                output[offset:offset + bgzf.SIZEOF_EMPTY_BLOCK] = bgzf.EMPTY_BLOCK
            self._offset = offset + bgzf.SIZEOF_EMPTY_BLOCK
            self._output = None


class Writer(_Writer):
    @staticmethod
    def bgzf(output, offset=0, sam_header=b'', references=()):
        writer = BGZFWriter(output, offset, references)
        writer._output(bam.pack_header(sam_header, references))
        writer._output.finish_block()
        return writer
