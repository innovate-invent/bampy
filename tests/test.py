from bampy.util import open_buffer
from bampy import Reader, Writer


stream_in = open("normal.bam", 'rb')
stream_out = open("stream.bam", 'wb')
stream_out_sam = open("stream.sam", 'wb')
#buffer_in = open_buffer("normal.bam", 'r')
buffer_out = open_buffer("buffer.bam", size=32 * 2 ** 20)
buffer_out_sam = open_buffer("buffer.sam", size=32 * 2 ** 20 * 8)
offset = 0

stream_reader = Reader(stream_in)

header, ref = stream_reader.header, () #stream_reader.references

stream_writer = Writer.bgzf(stream_out, 0, header, ref)
buffer_writer = Writer.bgzf(buffer_out, 0, header, ref)

stream_writer_sam = Writer.sam(stream_out_sam, 0, header, ref)
buffer_writer_sam = Writer.sam(buffer_out_sam, 0, header, ref)

c = 0

for record in stream_reader:
    c += 1
    #stream_writer(record)
    #buffer_writer(record)
    stream_writer_sam(record)
    #buffer_writer_sam(record)

print(c)