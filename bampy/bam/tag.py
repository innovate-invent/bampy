import ctypes as C

from .util import InvalidBAM, _to_str

SIZEOF_UINT32 = C.sizeof(C.c_uint32)
SIZEOF_CHAR = C.sizeof(C.c_char)

BTAG_TYPES = {b'c': C.c_int8, b'C': C.c_uint8, b's': C.c_int16, b'S': C.c_uint16, b'i': C.c_int32, b'I': C.c_uint32, b'f': C.c_float}
TAG_TYPES = {b'A': C.c_char}
TAG_TYPES.update(BTAG_TYPES)

SIZEOF_TAG_TYPES = {k: C.sizeof(v) for k, v in TAG_TYPES.items()}


class TagHeader(C.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ("tag", C.c_char * 2),  # tag Two-character tag char[2]
        ("value_type", C.c_char),  # val_type Value type: AcCsSiIfZHB char
    ]

    def __len__(self):
        return SIZEOF_TAGHEADER


SIZEOF_TAGHEADER = C.sizeof(TagHeader)


class Tag:
    """
    Represents a record tag.
    """
    __slots__ = '_header', '_buffer'

    def __init__(self, buffer: memoryview or None, offset: int = 0):
        """
        Constructor.
        :param buffer: Buffer containing BAM formatted tag data or None.
        :param offset: Offset into the buffer pointing at the first byte of the tag identifier.
        """
        if buffer:
            self._header = TagHeader.from_buffer(buffer, offset)
            offset += SIZEOF_TAGHEADER
            if self._header.value_type in TAG_TYPES:
                self._buffer = TAG_TYPES[self._header.value_type].from_buffer(buffer, offset)
            elif self._header.value_type in b'ZH':
                start = offset
                while buffer[offset]: offset += 1  # Seek to null terminator
                offset += 1
                self._buffer = (C.c_char * (offset - start)).from_buffer(buffer, start)
            elif self._header.value_type == b'B':
                array_type = C.c_char.from_buffer(buffer, offset)
                offset += SIZEOF_CHAR
                length = C.c_uint32.from_buffer(buffer, offset)
                offset += SIZEOF_UINT32
                self._buffer = (BTAG_TYPES[array_type] * length).from_buffer(buffer, offset)
            else:
                raise InvalidBAM("Unknown tag value type.")
        else:
            self._header = TagHeader()
            self._buffer = None

    def size(self):
        """
        Calculates total memory occupied by tag.
        :return: Size in bytes of tag.
        """
        if self._buffer is not None:
            length = SIZEOF_TAGHEADER
            if self._header.value_type == b'B':
                # TODO make sure this is right, need data that uses B to verify
                length += SIZEOF_UINT32 + (len(self._buffer))
            elif self._header.value_type in b'HZ':
                length += len(self._buffer)
            else:
                length += SIZEOF_TAG_TYPES[self._header.value_type]
            return length
        else:
            return 0

    def __len__(self):
        """
        Length of data stored by tag.
        :return: Number of elements in array types (ZBH), 1 for all others, or 0 if tag has no value.
        """
        if self._buffer is not None:
            if self._header.value_type in b'ZBH':
                return len(self._buffer)
            else:
                return 1
        else:
            return 0

    @staticmethod
    def from_sam(column):
        """
        Load from SAM formatted tag.
        :param column: ASCII encoded bytes containing SAM formatted tag.
        :return: Instance of Tag representing column data.
        """
        tag = Tag.__new__(Tag)
        header = TagHeader()
        tag._header = header
        header.tag, value_type, tag._buffer = column.split(b':', 2)
        header.value_type = int(value_type)
        return tag

    def __repr__(self):
        """
        Convert to string representation of Tag.
        See __bytes__() to convert to SAM format.
        :return: str instance containing tag data.
        """
        return "{}:{}:{}".format(self._header.tag.decode('ASCII'),
                                 self._header.value_type.decode('ASCII') if self._header.value_type in b'AifZHB' else 'i', _to_str(self._buffer))

    def __bytes__(self):
        """
        Convert tag to SAM formatted bytes.
        :return: ASCII encoded bytes representing tag in SAM format.
        """
        if self._header.value_type in b'ZH':
            value = self._buffer[:-1]  # Omit the trailing Null
        elif self._header.value_type in b'AB':
            value = self._buffer
        else:
            value = str(self._buffer.value).encode('ASCII')
        return self._header.tag + b':' + (self._header.value_type if self._header.value_type in b'AifZHB' else b'i') + b':' + value

    def pack(self):
        """
        Convert to a BAM formatted bytes representation of the tag.
        :return: Bytearray object containing the tag
        """
        # TODO Avoid copying data
        return bytearray(self._header) + bytearray(self._buffer)

    def __getattr__(self, item):
        return getattr(self._header, item)

    def __getitem__(self, i):
        if self._header.value_type in b'ZBH':
            if self._buffer:
                return self._buffer[i]
            else:
                raise ValueError("Buffer not initialised.")
        else:
            raise TypeError('{} tag type is not subscriptable.'.format(TAG_TYPES[self._header.value_type]))

    def __setitem__(self, i, value):
        if self._buffer:
            if self._header.value_type in b'ZBH':
                if isinstance(i, slice):
                    start = i.start or 0
                    stop = i.stop or len(self)
                    step = i.step or 1
                    if hasattr(value, '__iter__') and len(value) == (stop - start) // step:
                        self._buffer[i] = value
                    else:
                        raise ValueError("Slice assignment can not change length of sequence.")
                else:
                    self._buffer[i] = value
            else:
                if isinstance(i, slice):
                    raise IndexError("{} tag type not indexable.".format(self._header.tag))
                self._buffer = value
        else:
            raise ValueError("Buffer not initialised.")

    def copy(self):
        """
        Duplicate the tag instance and underlying buffer.
        :return: New tag instance with copied data.
        """
        new = Tag.__new__(Tag)
        new._header = TagHeader.from_buffer_copy(self._header)
        new._buffer = bytearray(self._buffer)
        return new
