import ctypes as C
from ..reference import Reference
from .. import sam

MAGIC = b'BAM\x01'


def is_bam(buffer, offset = 0):
    return buffer[offset:offset+4] == MAGIC

class InvalidBAM(ValueError):
    pass

class BufferUnderflow(ValueError):
    pass

def _qscore_to_str(data):
    return ''.join(map(lambda x:chr(x+33), data))

def _to_bytes(data):
    return data.value

def _to_str(data):
    if isinstance(data.value, bytes):
        return data.value.decode('ASCII')
    else:
        return str(data.value)

def header_from_stream(stream, _magic = None):
    # Provide a friendly way of peeking into a stream for data type discovery
    if not _magic:
        magic = bytearray(4)
        stream.readinto(magic)
    if not is_bam(_magic or magic):
        raise InvalidBAM("Invalid BAM header found.")

    header_length = bytearray(C.sizeof(C.c_int32))
    stream.readinto(header_length)
    header_length = C.c_int32.from_buffer(header_length)

    header = bytearray(header_length)
    stream.readinto(header)
    #header = (C.c_char * header_length).from_buffer(header)

    ref_count = bytearray(C.sizeof(C.c_int32))
    stream.readinto(ref_count)
    ref_count = C.c_int32.from_buffer(ref_count)

    # List of reference information (n=n ref )
    refs = []
    for _ in range(ref_count):
        length = bytearray(C.sizeof(C.c_int32))
        stream.readinto(length)
        length = C.c_int32.from_buffer(length)  # l_name Length of the reference name plus 1 (including NUL) int32 t
        name = bytearray(length)                     # name Reference sequence name; NUL-terminated char[l name]
        stream.readinto(name)
        seq_length = bytearray(C.sizeof(C.c_int32))
        stream.readinto(seq_length)
        seq_length = C.c_int32.from_buffer(seq_length)  # l_ref Length of the reference sequence int32 t
        refs.append(Reference(name.decode('ASCII'), seq_length.value))
    return header, refs, 0

def header_from_buffer(buffer, offset = 0):
    buffer_len = len(buffer)
    magic = (C.c_char * 4).from_buffer(buffer, offset)             # magic BAM magic string char[4] BAM\1
    if magic.raw != MAGIC:
        raise InvalidBAM("Invalid BAM header found.")
    offset += 4

    header_length = C.c_int32.from_buffer(buffer, offset).value          # l_text Length of the header text, including any NUL padding int32 t
    if buffer_len < offset + header_length:
        raise BufferUnderflow()
    offset += C.sizeof(C.c_int32)

    header = (C.c_char * header_length).from_buffer(buffer, offset) # text Plain header text in SAM; not necessarily NUL-terminated char[l text]
    offset += header_length

    ref_count = C.c_int32.from_buffer(buffer, offset).value              # n_ref # reference sequences int32 t
    offset += C.sizeof(C.c_int32)

    # List of reference information (n=n ref )
    refs = []
    for _ in range(ref_count):
        length = C.c_int32.from_buffer(buffer, offset).value         # l_name Length of the reference name plus 1 (including NUL) int32 t
        if buffer_len < offset + length:
            raise BufferUnderflow()
        offset += C.sizeof(C.c_int32)
        name = (C.c_char * length).from_buffer(buffer, offset) # name Reference sequence name; NUL-terminated char[l name]
        offset += length
        seq_length = C.c_int32.from_buffer(buffer, offset)     # l_ref Length of the reference sequence int32 t
        offset += C.sizeof(C.c_int32)
        refs.append(Reference(_to_str(name), seq_length.value))
    return header.raw, refs, offset

def pack_header(sam_header=b'', references=()) -> bytearray:
    sam_header = sam.pack_header(sam_header, references)
    bam_header = bytearray(MAGIC)
    l_text = len(sam_header)
    bam_header += (l_text.to_bytes(C.sizeof(C.c_int32), 'little', signed=True)
                   + sam_header
                   + len(references).to_bytes(C.sizeof(C.c_int32), 'little', signed=True))
    for ref in references:
        bam_header += ref.pack()
    return bam_header

def header_to_stream(stream, sam_header=b'', references=()):
    stream.write(pack_header(sam_header, references))
    return 0

def header_to_buffer(buffer, offset=0, sam_header=b'', references=()):
    header = pack_header(sam_header, references)
    end = offset + len(header)
    buffer[offset : end] = header
    return end + 1
