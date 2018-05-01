import numba as nb

import bampy.bgzf.zlib as zlib
from bampy.bgzf.zlib import DEFAULT_COMPRESSION_LEVEL, Z_BEST_SPEED
from bampy.mt import CACHE_JIT

raw_decompress = nb.jit(nb.types.Tuple((nb.intc, zlib.zState))(nb.optional(memoryview), nb.optional(memoryview), nb.intc, nb.optional(zlib.zState), nb.intc, nb.optional(bytes)), locals={'err':nb.intc, 'state':zlib.zState}, nopython=True, nogil=True, cache=CACHE_JIT)(zlib.raw_decompress)
raw_compress = nb.jit(nb.types.Tuple((nb.intc, zlib.zState))(nb.optional(memoryview), nb.optional(memoryview), nb.intc, nb.optional(zlib.zState), nb.intc, nb.intc, nb.intc, nb.optional(bytes)), locals={'err':nb.intc, 'state':zlib.zState}, nopython=True, nogil=True, cache=CACHE_JIT)(zlib.raw_compress)
