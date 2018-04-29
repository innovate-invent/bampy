from concurrent.futures import ThreadPoolExecutor
import numba, threading

from ..writer import Writer as _Writer, BGZFWriter as _BGZFWriter
from .. import bam
from . import bgzf

@numba.jit(nopython=True, nogil=True)
def push(self, record):
    data = record.pack()
    record_len = len(record)
    if record_len < bgzf.MAX_CDATA_SIZE and self._output.block_remaining() < record_len:
        self._output.finish_block()
    for datum in data:
        self._output(datum)


class BGZFWriter(_BGZFWriter):
    def __init__(self, output, offset=0, references=(), threadpool: ThreadPoolExecutor = ThreadPoolExecutor(thread_name_prefix='BAMPY_WORKER')):
        super().__init__(bgzf.Writer(output, offset, threadpool), references)

    def __call__(self, record):
        push(self, record)


class Writer(_Writer):
    @staticmethod
    def bgzf(output, offset=0, sam_header=b'', references=()):
        writer = BGZFWriter(output, offset, references)
        writer._output(bam.pack_header(sam_header, references))
        writer._output.finish_block()
        return writer

