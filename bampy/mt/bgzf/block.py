import numba

import bampy.bgzf as bgzf

SubField = numba.jitclass({
    'header': bgzf.block.SubFieldHeader,
    'data': bytearray,
})(bgzf.block.SubField)

Block = numba.jitclass({
    '_header': bgzf.block.Header,
    '_trailer': bgzf.block.Trailer,
    'extra_fields': SubField[:],
    'size': int,
    'flags': bgzf.block.BlockFlags,
})(bgzf.Block)
