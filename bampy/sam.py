import re
from collections import defaultdict

from . import bam

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

    return header, [bam.Reference(ref[b'SN'], int(ref[b'LN'])) for ref in header.pop(b'SQ')], 0

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

    return header, [bam.Reference(ref[b'SN'], int(ref[b'LN'])) for ref in header.pop(b'SQ')], offset

def pack_header(header, references=()):
    if isinstance(header, dict):
        HD = bytearray()
        buffer = bytearray()
        for tag, v in header.items():
            if tag == b'HD':
                HD = b'@HD\t' + b'\t'.join(attr + b':' + value for attr, value in v[0].items()) + b'\n'
            elif tag == b'CO':
                for line in v:
                    buffer += b'@CO\t' + line + b'\n'
            else:
                for line in v:
                    buffer += b'@' + tag + bytes(b'\t' + attr + b':' + value for attr, value in line.items()) + b'\n'

        assert HD, "No HD tag provided."
        assert header[b'SQ'] or references, "No references provided."
        header = HD + buffer

    for ref in references:
        header += bytes(ref)

    return header