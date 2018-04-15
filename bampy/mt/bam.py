import bampy.reference as reference
import bampy.bam as bam
import bampy.bam.packed_sequence as packed_sequence
import bampy.bam.packed_cigar as packed_cigar
import numba

Reference = numba.jitclass({
    'name' : str,
    'length' : int,
    'index' : int,
    '_optional' : dict,
})(reference.Reference)

PackedCIGAR = numba.jitclass({
    'buffer' : memoryview,
})(packed_cigar.PackedCIGAR)

PackedSequence = numba.jitclass({
    'buffer' : memoryview,
    '_length' : int,
})(packed_sequence.PackedSequence)

Record = numba.jitclass({
    '_header' : bam.record.RecordHeader,
    'name' : bytearray,
    'cigar' : PackedCIGAR,
    'sequence' : PackedSequence,
    'quality_scores' : numba.char[:],
    'tags' : dict,
    'reference' : Reference,
    'next_reference' : Reference,
})(bam.record.Record)