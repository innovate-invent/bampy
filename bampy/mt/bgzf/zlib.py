import numba
import bampy.bgzf.zlib as zlib

raw_decompress = numba.jit(nopython=True, nogil=True)(zlib.raw_decompress)
raw_compress = numba.jit(nopython=True, nogil=True)(zlib.raw_compress)