import re
from collections import defaultdict

from . import bam

header_re = re.compile(r"\t([A-Za-z][A-Za-z0-9]):([ -~]+)")
cigar_re = re.compile(r"([0-9]+)([MIDNSHPX=])")


def header_from_stream(stream, _magic=None) -> (dict, list, int):
    """
    Parse SAM formatted header from stream.
    Dict of header values returned is structured as such: {Header tag:[ {Attribute tag: value}, ]}.
    Header tags can occur more than once and so each list item represents a different tag line.
    :param stream: Stream containing header data.
    :param _magic: Data consumed from stream while peeking. Will be prepended to read data.
    :return: Tuple containing (Dict of header values, list of Reference objects, placeholder to keep return value consistent with header_from_buffer()).
    """
    header = defaultdict(list)
    while stream.peek(1)[0] == b'@':
        line = stream.readline()
        tag = line[1:2]
        if tag == b'CO':
            header[tag].append(line[4:])
        else:
            header[tag].append({m[0]: m[1] for m in header_re.findall(line)})

    return header, [bam.Reference(ref[b'SN'], int(ref[b'LN'])) for ref in header.pop(b'SQ')], 0


def header_from_buffer(buffer, offset=0) -> (dict, list, int):
    """
    Parse SAM formatted header from buffer.
    Dict of header values returned is structured as such: {Header tag:[ {Attribute tag: value}, ]}.
    Header tags can occur more than once and so each list item represents a different tag line.
    :param buffer: Buffer containing header data.
    :param offset: Offset into buffer pointing to first byte of header data.
    :return: Tuple containing (Dict of header values, list of Reference objects, offset into buffer where header ends and record data begins).
    """
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


def pack_header(header, references=()) -> bytearray:
    """
    Convert dict object returned by sam.header_from_buffer and sam.header_from_stream to a buffer.
    :param header: Dict containing header information or bytearray object.
    :param references: List of Reference object to append to the header.
    :return: Bytearray containing header data.
    """
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
