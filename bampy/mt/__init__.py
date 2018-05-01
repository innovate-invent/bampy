import distutils.util
import os


CACHE_JIT = distutils.util.strtobool(os.getenv('PYTHON_CACHEJIT', 'False'))
THREAD_NAME = 'BAMPY_WORKER'
DEFAULT_THREADS = len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else os.cpu_count()

from .reader import Reader
from .writer import Writer

# TODO Better use of numbas parallelization features
