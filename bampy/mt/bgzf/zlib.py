import numba

import bampy.bgzf.zlib as zlib
from bampy.mt import CACHE_JIT

raw_decompress = numba.jit(nopython=True, nogil=True, cache=CACHE_JIT)(zlib.raw_decompress)
raw_compress = numba.jit(nopython=True, nogil=True, cache=CACHE_JIT)(zlib.raw_compress)
