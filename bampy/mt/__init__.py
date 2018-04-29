from .reader import Reader
from .writer import Writer

from .bam import Record, OP_CODES, SEQUENCE_VALUES, CONSUMES_QUERY, CONSUMES_REFERENCE
from ..reader import discover_stream
from ..reference import Reference

import distutils.util, os


CACHE_JIT = distutils.util.strtobool(os.getenv('PYTHON_CACHEJIT', 'True'))
THREAD_NAME = 'BAMPY_WORKER'

#TODO Better use of numbas parallelization features