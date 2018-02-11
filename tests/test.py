from bampy.util import open_buffer
from bampy import Reader, Writer, bam
import gc

if True:
    stream_in = open("normal.bam", 'rb')
    stream_out = open("stream.bgzf.bam", 'wb')
    #stream_out_sam = open("stream.sam", 'wb')
    #buffer_in = open_buffer("normal.bam", 'r')
    buffer_out = open_buffer("buffer.bgzf.bam", size=40 * 2 ** 20)
    #buffer_out_sam = open_buffer("buffer.sam", size=117 * 2 ** 20 * 8)
    offset = 0

    stream_reader = Reader(stream_in)

    header, ref = stream_reader.header, stream_reader.references

    #result = bam.header_from_buffer(bam.pack_header(header, ref))

    stream_writer = Writer.bgzf(stream_out, 0, header, ref)
    buffer_writer = Writer.bgzf(buffer_out, 0, header, ref)

    #stream_writer_sam = Writer.sam(stream_out_sam, 0, header, ())
    #buffer_writer_sam = Writer.sam(buffer_out_sam, 0, header, ())

    c = 0

    for record in stream_reader:
        c += 1
        stream_writer(record)
        buffer_writer(record)
        #stream_writer_sam(record)
        #buffer_writer_sam(record)

    buffer_writer.finalize()
    size = buffer_writer.offset
    del buffer_writer
    gc.collect()
    buffer_out.resize(size)
    #buffer_out_sam.resize(buffer_writer_sam.offset)

else:
    stream_in = open("buffer.bgzf.bam", 'rb')

    stream_reader = Reader(stream_in)

    header, ref = stream_reader.header, stream_reader.references

    c = 0

    for record in stream_reader:
        c += 1

print(c)