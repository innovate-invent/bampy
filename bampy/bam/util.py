import ctypes as C
from typing import Tuple
from enum import IntEnum

from .. import sam
from ..reference import Reference

SIZEOF_INT32 = C.sizeof(C.c_int32)

MAGIC = b'BAM\x01'
"""bytes: Magic bytes identifying BAM record"""

OP_CODES = tuple(b"MIDNSHP=X"[i:i + 1] for i in range(9))
"""tuple: ASCII encoded CIGAR operations indexed by their numeric op codes."""

SEQUENCE_VALUES = tuple(b"=ACMGRSVTWYHKDBN"[i:i + 1] for i in range(9))
"""tuple: ASCII encoded sequence values indexed by their numeric code."""

class CigarOps(IntEnum):
    """Enum of numeric CIGAR operations."""
    MATCH = 0 # M
    INS = 1 # I
    DEL = 2 # D
    REF_SKIP = 3 # N
    SOFT_CLIP = 4 # S
    HARD_CLIP = 5 # H
    PAD = 6 # P
    EQUAL = 7 # =
    DIFF = 8 # X
    BACK = 9 # B

CONSUMES_QUERY = (
    True,  # M
    True,  # I
    False,  # D
    False,  # N
    True,  # S
    False,  # H
    False,  # P
    True,  # =
    True,  # X
)
"""tuple: Boolean values ordered by op code indicating if op consumes a query sequence position."""

CONSUMES_REFERENCE = (
    True,  # M
    False,  # I
    True,  # D
    True,  # N
    False,  # S
    False,  # H
    False,  # P
    True,  # =
    True,  # X
)
"""tuple: Boolean values ordered by op code indicating if op consumes a reference position."""

CLIPPED = (
    False,  # M
    False,  # I
    False,  # D
    False,  # N
    True,  # S
    True,  # H
    False,  # P
    False,  # =
    False,  # X
)
"""tuple: Boolean values ordered by op code indicating if op is soft or hard clip."""

def is_bam(buffer, offset=0):
    """
    Helper to determine if passed buffer contains a BAM record.
    :param buffer: Buffer containing unknown data.
    :param offset: Offset into buffer to being reading.
    :return: True if offset points to beginning of a BAM record, False otherwise.
    """
    return buffer[offset:offset + 4] == MAGIC


class InvalidBAM(ValueError):
    """
    Exception to indicate invalid or unexpected data was read while trying to parse BAM formatted data.
    """
    pass


class BufferUnderflow(ValueError):
    """
    Exception to indicate that the buffer being read does not contain enough data to finish reading a unit of BAM formatted data.
    """
    pass


def _qscore_to_str(data):
    """
    Monkey-patch used for debugging QSCORE data
    :param data:
    :return:
    """
    return ''.join(map(lambda x: chr(x + 33), data))


def _to_bytes(data):
    """
    Monkey-patch used for debugging ctypes objects buffer data.
    :param data:
    :return:
    """
    return data.value


def _to_str(data):
    """
    Monkey-patch used for debugging ctypes objects ASCII encoded string data.
    :param data:
    :return:
    """
    if isinstance(data.value, bytes):
        return data.value.decode('ASCII')
    else:
        return str(data.value)


def header_from_stream(stream, _magic=None) -> Tuple[bytearray, list, int]:
    """
    Read in BAM header data.
    Note: SAM formatted header data will likely contain duplicate reference data.
    :param stream: Stream containing header data.
    :param _magic: Data consumed from stream while peeking. Will be prepended to read data.
    :return: Tuple containing (Bytes object containing SAM formatted header, list of Reference objects, placeholder to keep return value consistent with header_from_buffer())
    """
    # Provide a friendly way of peeking into a stream for data type discovery
    if not _magic:
        magic = bytearray(4)
        stream.readinto(magic)
    if not is_bam(_magic or magic):
        raise InvalidBAM("Invalid BAM header found.")

    header_length = bytearray(SIZEOF_INT32)
    assert stream.readinto(header_length) == 4
    header_length = int.from_bytes(header_length, byteorder='little', signed=True)  # C.c_int32.from_buffer(header_length)

    header = bytearray(header_length)
    assert stream.readinto(header) == header_length
    # header = (C.c_char * header_length).from_buffer(header)

    ref_count = bytearray(SIZEOF_INT32)
    stream.readinto(ref_count)
    ref_count = int.from_bytes(ref_count, byteorder='little', signed=True)  # C.c_int32.from_buffer(ref_count)

    # List of reference information (n=n ref )
    refs = []
    for i in range(ref_count):
        length = bytearray(SIZEOF_INT32)
        assert stream.readinto(length) == SIZEOF_INT32
        length = int.from_bytes(length, byteorder='little',
                                signed=True)  # C.c_int32.from_buffer(length)  # l_name Length of the reference name plus 1 (including NUL) int32 t
        name = bytearray(length)  # name Reference sequence name; NUL-terminated char[l name]
        stream.readinto(name)
        seq_length = bytearray(SIZEOF_INT32)
        stream.readinto(seq_length)
        seq_length = int.from_bytes(seq_length, byteorder='little',
                                    signed=True)  # C.c_int32.from_buffer(seq_length)  # l_ref Length of the reference sequence int32 t
        refs.append(Reference(name.decode('ASCII'), seq_length, i))
    return header, refs, 0


def header_from_buffer(buffer, offset=0) -> Tuple[bytearray, list, int]:
    """
    Read in BAM header data.
    Note: SAM formatted header data will likely contain duplicate reference data.
    :param buffer: Buffer containing header data.
    :param offset: Offset into buffer pointing to first byte of header data.
    :return: Tuple containing (Bytes object containing SAM formatted header, list of Reference objects, offset into buffer where header ends and record data begins)
    """
    buffer_len = len(buffer)
    magic = (C.c_char * 4).from_buffer(buffer, offset)  # magic BAM magic string char[4] BAM\1
    if magic.raw != MAGIC:
        raise InvalidBAM("Invalid BAM header found.")
    offset += 4

    header_length = C.c_int32.from_buffer(buffer, offset).value  # l_text Length of the header text, including any NUL padding int32 t
    if buffer_len < offset + header_length:
        raise BufferUnderflow()
    offset += SIZEOF_INT32

    header = (C.c_char * header_length).from_buffer(buffer, offset)  # text Plain header text in SAM; not necessarily NUL-terminated char[l text]
    offset += header_length

    ref_count = C.c_int32.from_buffer(buffer, offset).value  # n_ref # reference sequences int32 t
    offset += SIZEOF_INT32

    # List of reference information (n=n ref )
    refs = []
    for i in range(ref_count):
        length = C.c_int32.from_buffer(buffer, offset).value  # l_name Length of the reference name plus 1 (including NUL) int32 t
        if buffer_len < offset + length:
            raise BufferUnderflow()
        offset += SIZEOF_INT32
        name = (C.c_char * length).from_buffer(buffer, offset)  # name Reference sequence name; NUL-terminated char[l name]
        offset += length
        seq_length = C.c_int32.from_buffer(buffer, offset)  # l_ref Length of the reference sequence int32 t
        offset += SIZEOF_INT32
        refs.append(Reference(_to_str(name), seq_length.value, i))
    return header.raw, refs, offset


def pack_header(sam_header=b'', references=()) -> bytearray:
    """
    Generate BAM header.
    :param sam_header: ASCII encoded SAM header to include.
    :param references: List of Reference objects. Order of list determines record index id's.
    :return: bytearray object containing BAM formatted header.
    """
    sam_header = sam.pack_header(sam_header, references)
    bam_header = bytearray(MAGIC)
    l_text = len(sam_header)
    bam_header += (l_text.to_bytes(SIZEOF_INT32, 'little', signed=True)
                   + sam_header
                   + len(references).to_bytes(SIZEOF_INT32, 'little', signed=True))
    for ref in references:
        bam_header += ref.pack()
    return bam_header


def header_to_stream(stream, sam_header=b'', references=()) -> int:
    """
    Write BAM formatted header to stream.
    :param stream: Stream to write to.
    :param sam_header: ASCII encoded SAM header to include.
    :param references: List of Reference objects. Order of list determines record index id's.
    :return: int(0)
    """
    stream.write(pack_header(sam_header, references))
    return 0


def header_to_buffer(buffer, offset=0, sam_header=b'', references=()) -> int:
    """
    Write BAM formatted header to buffer.
    :param buffer: buffer to write to.
    :param offset: Offset into buffer to begin writing from.
    :param sam_header: ASCII encoded SAM header to include.
    :param references: List of Reference objects. Order of list determines record index id's.
    :return: int(0)
    """
    header = pack_header(sam_header, references)
    end = offset + len(header)
    buffer[offset: end] = header
    return end + 1


def alignment_length(cigar):
    """
    Count number of reference consuming positions that CIGAR represents.
    :param cigar: Iterable returning tuples of the form (op length, op).
    :return: Total alignment length of CIGAR.
    """
    total = 0
    for count, op in cigar:
        if CONSUMES_REFERENCE[op]:
            total += count
    return total


def reg2bin(beg, end):
    """
    Calculate bin given an alignment covering [beg,end) (zero-based, half-closed-half-open)
    Adapted directly from SAM spec.
    :param beg:
    :param end:
    :return:
    """
    end -= 1
    if (beg >> 14 == end >> 14): return ((1 << 15) - 1) // 7 + (beg >> 14)
    if (beg >> 17 == end >> 17): return ((1 << 12) - 1) // 7 + (beg >> 17)
    if (beg >> 20 == end >> 20): return ((1 << 9) - 1) // 7 + (beg >> 20)
    if (beg >> 23 == end >> 23): return ((1 << 6) - 1) // 7 + (beg >> 23)
    if (beg >> 26 == end >> 26): return ((1 << 3) - 1) // 7 + (beg >> 26)
    return 0


MAX_BIN = (((1 << 18) - 1) / 7)


def reg2bins(beg, end):
    """
    Calculate the list of bins that may overlap with region [beg,end) (zero-based)
    Adapted directly from SAM spec.
    :param beg:
    :param end:
    :return:
    """
    end -= 1
    bins = [0]
    for k in range(1 + (beg >> 26), 2 + (end >> 26)): bins.append(k)
    for k in range(9 + (beg >> 23), 10 + (end >> 23)): bins.append(k)
    for k in range(73 + (beg >> 20), 74 + (end >> 20)): bins.append(k)
    for k in range(585 + (beg >> 17), 586 + (end >> 17)): bins.append(k)
    for k in range(4681 + (beg >> 14), 4682 + (end >> 14)): bins.append(k)
    return bins
