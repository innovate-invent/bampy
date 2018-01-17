import re
from collections import defaultdict

import bampy.bam as bam

header_re = re.compile(r"\t([A-Za-z][A-Za-z0-9]):([ -~]+)")
cigar_re = re.compile(r"([0-9]+)([MIDNSHPX=])")

def header_from_stream(stream, _magic = None):
    header = defaultdict(list)
    while stream.peek(1)[0] == b'@':
        line = stream.readline()
        tag = line[1:2]
        if tag == b'CO':
            header[tag].append(line[4:])
        else:
            header[tag].append({m[0]:m[1] for m in header_re.findall(line)})

    return header, [bam.Reference(ref[b'SN'], int(ref[b'LN'])) for ref in header[b'SQ']], 0


def header_from_buffer(buffer, offset = 0):
    header = defaultdict(list)
    while buffer[offset] == b'@':
        end = buffer.find(b'\n', offset)
        line = buffer[offset:end]
        tag = line[1:2]
        if tag == b'CO':
            header[tag].append(line[4:])
        else:
            header[tag].append({m[0]: m[1] for m in header_re.findall(line)})
        offset = end + 1

    return header, [bam.Reference(ref[b'SN'], int(ref[b'LN'])) for ref in header[b'SQ']], offset

def streamReader(references, stream):
    while True:
        yield bam.Record.from_sam(stream.readline(), references)

def bufferReader(references, buffer, offset = 0):
    while offset < len(buffer):
        end = buffer.find(b'\n', offset)
        yield bam.Record.from_sam(buffer[offset, end], references)
        offset = end + 1

def streamWriter(records, stream):
    for record in records:
        stream.writeline(repr(record))

def bufferWriter(records, buffer, offset = 0):
    for record in records:
        r = repr(record)
        l = len(r)
        buffer[offset:offset + l] = r
        offset += l
    return offset
