from bampy.util import streamReader_BGZF

stream_in = open("normal.bam", 'rb')
stream_out = open("normal.bam", 'rb')
buffer_in = open("normal.bam", 'rb')
buffer_out = open("normal.bam", 'rb')
count = 0
for record in streamReader_BGZF(stream_in): count += 1
print(count)