import numba

import bampy.bam as bam
import bampy.bam.packed_cigar as packed_cigar
import bampy.bam.packed_sequence as packed_sequence
import bampy.bam.tag as tag
import bampy.reference as reference

Tag = numba.jitclass({
    '_header': tag.TagHeader,
    '_buffer': memoryview,
})(bam.Tag)

"""Reference = numba.jitclass({
    'name' : str,
    'length' : int,
    'index' : int,
    '_optional' : dict,
})(reference.Reference)"""

PackedCIGAR = numba.jitclass({
    'buffer': memoryview,
})(packed_cigar.PackedCIGAR)

PackedSequence = numba.jitclass({
    'buffer': memoryview,
    '_length': int,
})(packed_sequence.PackedSequence)

Record = numba.jitclass({
    '_header': bam.record.RecordHeader,
    '_name': numba.optional(bytearray),
    '_cigar': numba.optional(PackedCIGAR),
    '_sequence': numba.optional(PackedSequence),
    '_quality_scores': numba.optional(numba.char[:]),
    '_tags': numba.optional(Tag[:]),
    '_reference': numba.optional(reference.Reference),
    '_next_reference': numba.optional(reference.Reference),
})(bam.Record)
