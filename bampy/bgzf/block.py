import ctypes as C
from .util import InvalidBGZF, MAX_BLOCK_SIZE
from enum import IntFlag

# Taken from gzip spec
class BlockFlags(IntFlag):
    FTEXT       = 1 << 0
    FHCRC       = 1 << 1
    FEXTRA      = 1 << 2
    FNAME       = 1 << 3
    FCOMMENT    = 1 << 4
    reserved1   = 1 << 5
    reserved2   = 1 << 6
    reserved3   = 1 << 7

class ExtraFlags(IntFlag):
    MAX_COMPRESSION = 1 << 2
    FASTEST         = 1 << 4

#TODO Add check for other block flags
class Header(C.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ("id1", C.c_uint8),    # ID1   gzip IDentifier1            uint8 31
        ("id2", C.c_uint8),    # ID2   gzip IDentifier2            uint8 139
        ("compression_method", C.c_uint8),     # CM    gzip Compression Method     uint8 8
        ("flag", C.c_uint8),    # FLG   gzip FLaGs                  uint8 4
        ("modification_time", C.c_uint32),  # MTIME gzip Modification TIME      uint32
        ("extra_flags", C.c_uint8),    # XFL   gzip eXtra FLags            uint8
        ("os", C.c_uint8),     # OS    gzip Operating System       uint8
        ("extra_length", C.c_uint16)   # XLEN  gzip eXtra LENgth           uint16
    ]


SIZEOF_HEADER = C.sizeof(Header)

# Extra subfield(s) (total size=XLEN)
#   Additional RFC1952 extra subfields if present
class SubField(C.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ("SI1", C.c_uint8), #   SI1 Subfield Identifier1        uint8 66
        ("SI2", C.c_uint8), #   SI2 Subfield Identifier2        uint8 67
        ("SLEN", C.c_uint16) #   SLEN Subfield LENgth uint16 t 2
    ]

class BSIZE(SubField):
    _pack_ = 1
    _fields_ = [
        ("value", C.c_uint16)
    ]
    def __init__(self):
        self.SI1 = 66
        self.SI2 = 67
        self.SLEN = C.sizeof(C.c_uint16)

class FixedXLENHeader(Header):
    _pack_ = 1
    _fields_ = [
        ("BSIZE", BSIZE)
    ]
    def __init__(self):
        self.id1 = 31
        self.id2 = 139
        self.compression_method = 8
        self.flag = BlockFlags.FEXTRA
        self.modification_time = 0
        self.extra_flags = ExtraFlags.MAX_COMPRESSION
        self.os = 255
        self.extra_length = C.sizeof(BSIZE)

class Trailer(C.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ("CRC32", C.c_uint32),              # CRC32 CRC-32                      uint32
        ("uncompressed_size", C.c_uint32)   # ISIZE Input SIZE (length of uncompressed data) uint32
    ]

MAX_CDATA_SIZE = MAX_BLOCK_SIZE - C.sizeof(FixedXLENHeader) - C.sizeof(Trailer)

class Block:
    __slots__ = '_header', '_trailer', 'extra_fields', 'size', 'flags'

    def __init__(self, header: Header, extra_fields: dict, trailer: Trailer):
        self._header = header
        self.flags = BlockFlags(header.flag)
        self.extra_fields = extra_fields
        self.size = Block._getSize(extra_fields)
        self._trailer = trailer

    def __getattr__(self, item):
        #TODO @properties might be faster
        if item not in ('_header', '_trailer'):
            try:
                return getattr(self._header, item)
            except AttributeError:
                return getattr(self._trailer, item)

    def __setattr__(self, key, value):
        if hasattr(self._header, key):
            setattr(self._header, key, value)
        elif hasattr(self._trailer, key):
            setattr(self._trailer, key, value)
        else:
            super().__setattr__(key, value)

    def __len__(self):
        return self.size

    @staticmethod
    def from_buffer(buffer, offset = 0) -> ('Block', memoryview):
        start = offset
        buffer = memoryview(buffer)
        header = Header.from_buffer(buffer, offset)
        if header.id1 != 31 and header.id2 != 139:
            raise InvalidBGZF("Invalid block header found: ID1: {} ID2: {}".format(header.id1, header.id2))

        # Parse extra fields
        offset += C.sizeof(Header)
        extra_fields = Block._parseExtra(buffer[offset : offset + header.extra_length])

        offset += header.extra_length
        block_size = Block._getSize(extra_fields)
        trailer_start = start + block_size - C.sizeof(Trailer)
        trailer = Trailer.from_buffer(buffer, trailer_start)

        return Block(header, extra_fields, trailer), buffer[offset : trailer_start]

    @staticmethod
    def from_stream(stream, _magic = None) -> ('Block', memoryview):
        # Provide a friendly way of peeking into a stream for data type discovery
        if _magic:
            header_buffer = bytearray(SIZEOF_HEADER - len(_magic))
            header_len = stream.readinto(header_buffer) + len(_magic)
            header_buffer = _magic + header_buffer
        else:
            header_buffer = bytearray(SIZEOF_HEADER)
            header_len = stream.readinto(header_buffer)
        if header_len == 0:
            raise EOFError()
        assert header_len == SIZEOF_HEADER
        header = Header.from_buffer(header_buffer)
        if header.id1 != 31 and header.id2 != 139:
            raise InvalidBGZF("Invalid block header found: ID1: {} ID2: {}".format(header.id1, header.id2))

        extra_fields_buffer = bytearray(header.extra_length)
        assert stream.readinto(extra_fields_buffer) == header.extra_length
        extra_fields = Block._parseExtra(extra_fields_buffer)

        data_size = Block._getSize(extra_fields) - SIZEOF_HEADER - C.sizeof(Trailer) - header.extra_length
        buffer = memoryview(bytearray(data_size))

        assert stream.readinto(buffer) == data_size

        trailer = bytearray(C.sizeof(Trailer))
        assert stream.readinto(trailer) == C.sizeof(Trailer)
        trailer = Trailer.from_buffer(trailer)

        return Block(header, extra_fields, trailer), buffer

    @staticmethod
    def _parseExtra(buffer):
        extraFields = {}
        fieldOffset = 0
        while fieldOffset < len(buffer):
            field = SubField.from_buffer(buffer, fieldOffset)
            fieldStart = fieldOffset + C.sizeof(SubField)
            extraFields[bytes((field.SI1, field.SI2))] = buffer[fieldStart: fieldStart + field.SLEN]
            fieldOffset += C.sizeof(SubField) + field.SLEN
        return extraFields

    @staticmethod
    def _getSize(extra_fields):
        # Load BGZF required BC field
        BC = extra_fields.get(b'BC')
        if BC:
            size = C.c_uint16.from_buffer_copy(BC).value + 1  # type: int
            if size is not None:
                return size
        raise InvalidBGZF("Missing block size field.")
