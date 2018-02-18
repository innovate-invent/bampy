"""
Python implementation of htslib supporting BAM, SAM, and BGZF compression.

Classes:
    Record: Represents an alignment record in memory
    Reader: Convenience interface for reading alignment records.
    Writer: Convenience interface for writing alignment records.
    Reference: Represents a reference sequence that the records were aligned to.

Functions:
    discover_stream: Used to determine the type of data in a stream.

Constants:
    OP_CODES (tuple): ASCII encoded CIGAR operations indexed by their numeric op codes.
    SEQUENCE_VALUES (tuple): ASCII encoded sequence values indexed by their numeric code.
    CONSUMES_QUERY (tuple): Boolean values ordered by op code indicating if op consumes a query sequence position.
    CONSUMES_REFERENCE (tuple): Boolean values ordered by op code indicating if op consumes a reference position.

For more:
    >> help(bampy.bam) for more information on working with BAM formatted data.
    >> help(bampy.bgzf) for more information on working with BGZF compressed data.
    >> help(bampy.reader) for more information on reading HTS alignment data.
    >> help(bampy.writer) for more information on writing HTS alignment data.
    >> help(bampy.sam) for more information on working with SAM formatted.
    >> help(bampy.util) for more information on misc utilities provided.
"""

from bampy.bam import Record, OP_CODES, SEQUENCE_VALUES, CONSUMES_QUERY, CONSUMES_REFERENCE
from .__version import __version__
from .reader import Reader, discover_stream
from .reference import Reference
from .writer import Writer

# TODO Document everything
# TODO bai
# TODO multithread
# TODO tools: view, sort,
# TODO figure out why bgzf compression not as good as samtools
# TODO Optimise
# TODO unit tests
