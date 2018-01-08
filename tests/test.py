from bampy.util import streamReader_BGZF

f = open("normal.bam", 'rb')
count = 0
for record in streamReader_BGZF(f): count += 1
print(count)