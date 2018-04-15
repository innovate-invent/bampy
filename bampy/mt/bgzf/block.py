import ctypes as C, numba
import bampy.bgzf as bgzf


Block = numba.jitclass({
    '_header' : bgzf.block.Header,
    '_trailer' : bgzf.block.Trailer,
    'extra_fields' : dict,
    'size' : int,
    'flags': bgzf.block.BlockFlags,
})(bgzf.Block)