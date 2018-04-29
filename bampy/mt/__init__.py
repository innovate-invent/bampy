import distutils.util
import os

from .reader import Reader
from .writer import Writer

CACHE_JIT = distutils.util.strtobool(os.getenv('PYTHON_CACHEJIT', 'True'))
THREAD_NAME = 'BAMPY_WORKER'

# TODO Better use of numbas parallelization features
