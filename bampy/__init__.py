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

Example 1:
    from bampy import Reader
    stream_in = open("data.bam", 'rb')
    stream_reader = Reader(stream_in)
    header, ref = stream_reader.header, stream_reader.references

    for record in stream_reader:
        ***Your logic here***

Example 2:
    from bampy.util import open_buffer
    from bampy import Reader, Writer
    import gc

    stream_in = open("data.bam", 'rb')
    stream_out = open("stream.bgzf.bam", 'wb')
    stream_out_sam = open("stream.sam", 'wb')
    buffer_out = open_buffer("buffer.bgzf.bam", size=40 * 2 ** 20)
    buffer_out_sam = open_buffer("buffer.sam", size=117 * 2 ** 20 * 8)
    offset = 0

    stream_reader = Reader(stream_in)

    header, ref = stream_reader.header, stream_reader.references

    stream_writer = Writer.bgzf(stream_out, 0, header, ref)
    buffer_writer = Writer.bgzf(buffer_out, 0, header, ref)

    stream_writer_sam = Writer.sam(stream_out_sam, 0, header, ())
    buffer_writer_sam = Writer.sam(buffer_out_sam, 0, header, ())

    for record in stream_reader:
        stream_writer(record)
        buffer_writer(record)
        stream_writer_sam(record)
        buffer_writer_sam(record)

    # Release all buffer references before resizing.
    buffer_writer.finalize()
    size = buffer_writer.offset
    del buffer_writer
    sam_size = buffer_writer_sam.offset
    del buffer_writer_sam
    gc.collect()
    buffer_out.resize(size)
    buffer_out_sam.resize(sam_size)


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
# TODO CRC in trailer
# TODO bai, csi
# TODO multithread
# TODO tools: view, sort,
# TODO figure out why bgzf compression not as good as samtools
# TODO Optimise
# TODO unit tests
