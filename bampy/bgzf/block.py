import ctypes as C
from enum import IntFlag

from . import zlib
from .util import InvalidBGZF, MAX_BLOCK_SIZE

SIZEOF_UINT16 = C.sizeof(C.c_uint16)
FIXED_XLEN_HEADER = b'\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00\x42\x43\x02\x00'


# Taken from gzip spec
class BlockFlags(IntFlag):
    FTEXT = 1 << 0
    FHCRC = 1 << 1
    FEXTRA = 1 << 2
    FNAME = 1 << 3
    FCOMMENT = 1 << 4
    reserved1 = 1 << 5
    reserved2 = 1 << 6
    reserved3 = 1 << 7


class ExtraFlags(IntFlag):
    MAX_COMPRESSION = 1 << 2
    FASTEST = 1 << 4


# TODO Add check for other block flags
class Header(C.LittleEndianStructure):
    """
    Represents BGZF/GZIP block header.
    """
    _pack_ = 1
    _fields_ = [
        ("id1", C.c_uint8),  # ID1   gzip IDentifier1            uint8 31
        ("id2", C.c_uint8),  # ID2   gzip IDentifier2            uint8 139
        ("compression_method", C.c_uint8),  # CM    gzip Compression Method     uint8 8
        ("flag", C.c_uint8),  # FLG   gzip FLaGs                  uint8 4
        ("modification_time", C.c_uint32),  # MTIME gzip Modification TIME      uint32
        ("extra_flags", C.c_uint8),  # XFL   gzip eXtra FLags            uint8
        ("os", C.c_uint8),  # OS    gzip Operating System       uint8
        ("extra_length", C.c_uint16)  # XLEN  gzip eXtra LENgth           uint16
    ]


SIZEOF_HEADER = C.sizeof(Header)


class SubField(C.LittleEndianStructure):
    """
    Represents a BGZF/GZIP block subfield header.
    """
    _pack_ = 1
    _fields_ = [
        ("SI1", C.c_uint8),  # SI1 Subfield Identifier1        uint8 66
        ("SI2", C.c_uint8),  # SI2 Subfield Identifier2        uint8 67
        ("SLEN", C.c_uint16)  # SLEN Subfield LENgth uint16 t 2
    ]


SIZEOF_SUBFIELD = C.sizeof(SubField)


class BSIZE(SubField):
    """
    Dedicated SubField subclass for the required block size field.
    """
    _pack_ = 1
    _fields_ = [
        ("value", C.c_uint16)
    ]

    def __init__(self):
        super().__init__(66, 67, SIZEOF_UINT16)


SIZEOF_BSIZE = C.sizeof(BSIZE)


class Trailer(C.LittleEndianStructure):
    """
    Represents BGZF/GZIP block trailer.
    """
    _pack_ = 1
    _fields_ = [
        ("CRC32", C.c_uint32),  # CRC32 CRC-32                      uint32
        ("uncompressed_size", C.c_uint32)  # ISIZE Input SIZE (length of uncompressed data) uint32
    ]


SIZEOF_TRAILER = C.sizeof(Trailer)
MAX_CDATA_SIZE = MAX_BLOCK_SIZE - len(FIXED_XLEN_HEADER) - 2 - SIZEOF_TRAILER
MAX_DATA_SIZE = zlib.default_bound_max(MAX_CDATA_SIZE)


class Block:
    """
    Represents BGZF/GZIP block.
    """
    __slots__ = '_header', '_trailer', 'extra_fields', 'size'

    def __init__(self, header: Header, extra_fields: dict, trailer: Trailer):
        """
        Constructor.
        :param header: Header object instance.
        :param extra_fields: Dictionary of extra fields keyed by the two byte identifier.
        :param trailer: Trailer object instance.
        """
        self._header = header
        self._extra_fields = extra_fields
        self._size = Block._getSize(extra_fields)
        self._trailer = trailer

    @property
    def id(self):
        return (self._header.id1, self._header.id2)

    @id.setter
    def id(self, value):
        self._header.id1, self._header.id2 = value

    @property
    def compression_method(self):
        return self._header.compression_method

    @compression_method.setter
    def compression_method(self, value):
        self._header.compression_method = value

    @property
    def flags(self):
        return BlockFlags(self._header.flag)

    @flags.setter
    def flags(self, value):
        self._header.flag = value

    @property
    def modification_time(self):
        return self._header.id1

    @modification_time.setter
    def modification_time(self, value):
        self._header.modification_time = value

    @property
    def extra_flags(self):
        return self._header.extra_flags

    @extra_flags.setter
    def extra_flags(self, value):
        self._header.extra_flags = value

    @property
    def os(self):
        return self._header.os

    @os.setter
    def os(self, value):
        self._header.os = value

    @property
    def extra_length(self):
        return self._header.extra_length

    @extra_length.setter
    def extra_flags(self, value):
        self._header.extra_length = value

    @property
    def CRC32(self):
        return self._footer.CRC32

    @CRC32.setter
    def CRC32(self, value):
        self._header.CRC32 = value

    @property
    def uncompressed_size(self):
        return self._footer.uncompressed_size

    @uncompressed_size.setter
    def uncompressed_size(self, value):
        self._header.uncompressed_size = value

    def __len__(self):
        return self.size

    @staticmethod
    def from_buffer(buffer, offset=0) -> ('Block', memoryview):
        """
        Load a block from a buffer.
        This references the buffer data and does not copy in memory.
        :param buffer: Buffer to read from.
        :param offset: Offset into buffer pointing to first block byte.
        :return: Tuple containing: (Block instance, memoryview containing compressed block data).
        """
        start = offset
        buffer = memoryview(buffer)
        header = Header.from_buffer(buffer, offset)
        if header.id1 != 31 and header.id2 != 139:
            raise InvalidBGZF("Invalid block header found: ID1: {} ID2: {}".format(header.id1, header.id2))

        # Parse extra fields
        offset += SIZEOF_HEADER
        extra_fields = Block._parseExtra(buffer[offset: offset + header.extra_length])

        offset += header.extra_length
        block_size = Block._getSize(extra_fields)
        trailer_start = start + block_size - SIZEOF_TRAILER
        trailer = Trailer.from_buffer(buffer, trailer_start)

        return Block(header, extra_fields, trailer), buffer[offset: trailer_start]

    @staticmethod
    def from_stream(stream, _magic=None) -> ('Block', memoryview):
        """
        Load a block from a stream.
        This copies the stream data into memory.
        :param stream: Stream to read from.
        :param _magic: Data consumed from stream while peeking. Will be appended to read data.
        :return: Tuple containing: (Block instance, memoryview containing compressed block data).
        """
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

        data_size = Block._getSize(extra_fields) - SIZEOF_HEADER - SIZEOF_TRAILER - header.extra_length
        buffer = memoryview(bytearray(data_size))

        assert stream.readinto(buffer) == data_size

        trailer = bytearray(SIZEOF_TRAILER)
        assert stream.readinto(trailer) == SIZEOF_TRAILER
        trailer = Trailer.from_buffer(trailer)

        return Block(header, extra_fields, trailer), buffer

    @staticmethod
    def _parseExtra(buffer) -> dict:
        """
        Parse GZIP formatted extra data fields into dictionary.
        :param buffer: Buffer containing extra field data.
        :return: Dict containing field values keyed on two byte field identifier.
        """
        extraFields = {}
        fieldOffset = 0
        while fieldOffset < len(buffer):
            field = SubField.from_buffer(buffer, fieldOffset)
            fieldStart = fieldOffset + SIZEOF_SUBFIELD
            extraFields[bytes((field.SI1, field.SI2))] = buffer[fieldStart: fieldStart + field.SLEN]
            fieldOffset += SIZEOF_SUBFIELD + field.SLEN
        return extraFields

    @staticmethod
    def _getSize(extra_fields) -> int:
        """
        Helper to parse BGZF block size subfield.
        :param extra_fields: Dict returned from _parseextra().
        :return: Total size of block.
        """
        # Load BGZF required BC field
        BC = extra_fields.get(b'BC')
        if BC:
            size = int.from_bytes(BC, byteorder='little', signed=False) + 1  # type: int
            if size is not None:
                return size
        raise InvalidBGZF("Missing block size field.")
