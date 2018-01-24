import ctypes as C
from . import InvalidBAM, _to_str

BTAG_TYPES = {b'c': C.c_int8, b'C': C.c_uint8, b's': C.c_int16, b'S': C.c_uint16, b'i': C.c_int32, b'I': C.c_uint32, b'f': C.c_float}
TAG_TYPES = {b'A': C.c_char}
TAG_TYPES.update(BTAG_TYPES)


class TagHeader(C.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ("tag", C.c_char * 2),  # tag Two-character tag char[2]
        ("value_type", C.c_char),  # val_type Value type: AcCsSiIfZHB char
    ]


class Tag:
    __slots__ = '_header', '_buffer'

    def __init__(self, buffer: memoryview or None, offset: int = 0):
        if buffer:
            self._header = TagHeader.from_buffer(buffer, offset)
            offset += C.sizeof(TagHeader)
            if self._header.value_type in TAG_TYPES:
                self._buffer = TAG_TYPES[self._header.value_type].from_buffer(buffer, offset)
            elif self._header.value_type in b'ZH':
                start = offset
                while buffer[offset]: offset += 1  # Seek to null terminator
                offset += 1
                self._buffer = (C.c_char * (offset - start)).from_buffer(buffer, start)
            elif self._header.value_type == b'B':
                array_type = C.c_char.from_buffer(buffer, offset)
                offset += C.sizeof(C.c_char)
                length = C.c_uint32.from_buffer(buffer, offset)
                offset += C.sizeof(C.c_uint32)
                self._buffer = (BTAG_TYPES[array_type] * length).from_buffer(buffer, offset)
            else:
                raise InvalidBAM("Unknown tag value type.")
        else:
            self._header = TagHeader()
            self._buffer = None

    def size(self):
        if self._buffer is not None:
            length = C.sizeof(TagHeader)
            if self._header.value_type == b'B':
                # TODO make sure this is right
                length += C.sizeof(C.c_uint32) + (len(self._buffer))
            elif self._header.value_type in b'HZ':
                length += len(self._buffer)
            else:
                length += C.sizeof(TAG_TYPES[self._header.value_type])
            return length
        else:
            return 0

    def __len__(self):
        if self._buffer is not None:
            if self._header.value_type in b'ZBH':
                return len(self._buffer)
            else:
                return 1
        else:
            return 0

    @staticmethod
    def from_sam(column):
        tag = Tag.__new__(Tag)
        header = TagHeader()
        tag._header = header
        header.tag, value_type, tag._buffer = column.split(b':', 2)
        header.value_type = int(value_type)
        return tag

    def __repr__(self):
        return "{}:{}:{}".format(self._header.tag.decode('ASCII'),
                                 self._header.value_type.decode('ASCII') if self._header.value_type in b'AifZHB' else 'i', _to_str(self._buffer))

    def __bytes__(self):
        return self._header.tag + b':' + (self._header.value_type if self._header.value_type in b'AifZHB' else b'i') + b':' + self._buffer

    def pack(self):
        return bytes(self._header) + bytes(self._buffer)

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
                self._buffer[i] = value
            else:
                self._buffer = value
        else:
            raise ValueError("Buffer not initialised.")

    def copy(self):
        new = Tag.__new__(Tag)
        new._header = TagHeader.from_buffer_copy(self._header)
        new._buffer = bytearray(self._buffer)
        return new
