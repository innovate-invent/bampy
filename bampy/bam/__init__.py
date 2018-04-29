"""
This subpackage contains all of the code required to work with BAM formatted data.

Classes:
    Record: The core representation of a BAM alignment record.
    Tag: Represents a record tag.
    CigarOps: Enum of numeric CIGAR operations.

Constants:
    OP_CODES (tuple): ASCII encoded CIGAR operations indexed by their numeric op codes.
    SEQUENCE_VALUES (tuple): ASCII encoded sequence values indexed by their numeric code.
    CONSUMES_QUERY (tuple): Boolean values ordered by op code indicating if op consumes a query sequence position.
    CONSUMES_REFERENCE (tuple): Boolean values ordered by op code indicating if op consumes a reference position.
    CLIPPED (tuple): Boolean values ordered by op code indicating if op is soft or hard clip.

For more:
    >> help(bampy.bam.record) for more information on the Record object.
    >> help(bampy.bam.packed_cigar) for more information on the PackedCIGAR object.
    >> help(bampy.bam.packed_sequence) for more information on the PackedSequence object.
    >> help(bampy.bam.tag) for more information on the Tag object.
    >> help(bampy.bam.util) for more information on utility functions including functions to work with BAM header data.
"""

from .record import Record
from .tag import Tag
from .util import CLIPPED, CONSUMES_QUERY, CONSUMES_REFERENCE, CigarOps, OP_CODES, SEQUENCE_VALUES, header_from_buffer, header_from_stream, \
    header_to_buffer, header_to_stream, is_bam, pack_header
