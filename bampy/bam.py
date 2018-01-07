import ctypes as C

SEQUENCE_VALUES = tuple("=ACMGRSVTWYHKDBN")
OP_CODES = tuple("MIDNSHP=X")
BTAG_TYPES = {b'c':C.c_int8, b'C':C.c_uint8, b's':C.c_int16, b'S':C.c_uint16, b'i':C.c_int32, b'I':C.c_uint32, b'f':C.c_float}

def isBAM(buffer):
    return buffer[:4] == b'BAM\x01'

class InvalidBAM(ValueError):
    pass

def header_from_stream(stream):
    magic = bytearray(4)
    stream.readinto(magic)
    if magic != b'BAM\x01':
        raise InvalidBAM("Invalid BAM header found.")
    header_length = bytearray(C.sizeof(C.c_int32))
    stream.readinto(header_length)
    header_length = C.c_int32.from_buffer(header_length)
    header = bytearray(header_length)
    stream.readinto(header)
    header = (C.c_char * header_length).from_buffer(header)
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
        refs.append((name, seq_length))
    return header, refs, 0

def header_from_buffer(buffer):
    offset = 0
    magic = (C.c_char * 4).from_buffer(buffer, offset)             # magic BAM magic string char[4] BAM\1
    if magic.raw != b'BAM\x01':
        raise InvalidBAM("Invalid BAM header found.")
    offset += 4
    header_length = C.c_int32.from_buffer(buffer, offset).value          # l_text Length of the header text, including any NUL padding int32 t
    offset += C.sizeof(C.c_int32)
    header = (C.c_char * header_length).from_buffer(buffer, offset)# text Plain header text in SAM; not necessarily NUL-terminated char[l text]
    offset += header_length
    ref_count = C.c_int32.from_buffer(buffer, offset).value              # n_ref # reference sequences int32 t
    offset += C.sizeof(C.c_int32)

    # List of reference information (n=n ref )
    refs = []
    for _ in range(ref_count):
        length = C.c_int32.from_buffer(buffer, offset).value         # l_name Length of the reference name plus 1 (including NUL) int32 t
        offset += C.sizeof(C.c_int32)
        name = (C.c_char * length).from_buffer(buffer, offset) # name Reference sequence name; NUL-terminated char[l name]
        offset += length
        seq_length = C.c_int32.from_buffer(buffer, offset)     # l_ref Length of the reference sequence int32 t
        offset += C.sizeof(C.c_int32)
        refs.append((name, seq_length))
    return header, refs, offset

class RecordHeader(C.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ("block_size", C.c_int32),         # block_size Length of the remainder of the alignment record int32 t
        ("reference_id", C.c_int32),       # refID Reference sequence ID, −1 ≤ refID < n ref; -1 for a read without a mapping position. int32 t [-1]
        ("position", C.c_int32),           # pos 0-based leftmost coordinate (= POS − 1) int32 t [-1]
        ("bin", C.c_int16),                # bin mq nl bin<<16|MAPQ<<8|l read name; bin is computed from the mapping position;18 l read name is the length of read name below (= length(QNAME) + 1). uint32 t
        ("mapping_quality", C.c_int8),     # bin mq nl bin<<16|MAPQ<<8|l read name; bin is computed from the mapping position;18 l read name is the length of read name below (= length(QNAME) + 1). uint32 t
        ("name_length", C.c_int8),         # bin mq nl bin<<16|MAPQ<<8|l read name; bin is computed from the mapping position;18 l read name is the length of read name below (= length(QNAME) + 1). uint32 t
        ("flag", C.c_int16),               # flag nc FLAG<<16|n_cigar op; n_cigar op is the number of operations in CIGAR. uint32 t
        ("cigar_length", C.c_int16),       # flag nc FLAG<<16|n_cigar op; n_cigar op is the number of operations in CIGAR. uint32 t
        ("sequence_length", C.c_int32),    # l_seq Length of SEQ int32 t
        ("next_reference_id", C.c_int32),  # next_refID Ref-ID of the next segment (−1 ≤ mate refID < n ref) int32 t [-1]
        ("next_position", C.c_int32),      # next_pos 0-based leftmost pos of the next segment (= PNEXT − 1) int32 t [-1]
        ("template_length", C.c_int32),    # tlen Template length (= TLEN) int32 t [0]
    ]

class PackedSequence:
    __slots__ = "_buffer", "_length"

    def __init__(self, buffer: memoryview or None, length):
        if buffer:
            self._buffer = buffer
        else:
            self._buffer = bytes((length + 1) / 2)

        self._length = length

    def __str__(self):
        seq = ""
        for c in self:
            seq += SEQUENCE_VALUES[c]
        return seq

    def __getitem__(self, i):
        if isinstance(i, slice):
            start = i.start or 0
            stop = i.stop or len(self)
            step = i.step or 1
            s = bytes((stop - start) / step)
            for a in range(start, stop, step):
                s[a] = self[a]
            return s

        if i % 2:
            return self._buffer[i/2] & 0b00001111
        else:
            return self._buffer[i/2] >> 4

    def __setitem__(self, i, value):
        if isinstance(i, slice):
            start = i.start or 0
            stop = i.stop or len(self)
            step = i.step or 1
            if hasattr(value, '__iter__') and len(value) == (stop - start) / step:
                #TODO optimise pairwise packing if step == 1
                for a, b in zip(range(start, stop, step), value):
                    if a % 2:
                        self._buffer[a / 2] = (self._buffer[a / 2] & 0b11110000) | b
                    else:
                        self._buffer[a / 2] = (self._buffer[a / 2] & 0b00001111) | (b << 4)
            else:
                #TODO make sequence mutable
                raise ValueError("Slice assignment can not change length of sequence.")
        else:
            if i % 2:
                self._buffer[i/2] = (self._buffer[i/2] & 0b11110000) | value
            else:
                self._buffer[i/2] = (self._buffer[i/2] & 0b00001111) | (value << 4)

    def __iter__(self):
        def it():
            for b in self._buffer:
                yield b >> 4
                yield b & 0b00001111
        return it

    def __reversed__(self):
        def it():
            for b in reversed(self._buffer):
                yield b & 0b00001111
                yield b >> 4
        return it

    def __len__(self):
        return self._length

    @staticmethod
    def pack(fromBuffer, toBuffer = None):
        if isinstance(fromBuffer, PackedSequence):
            #TODO memcpy to toBuffer
            return fromBuffer
        packed = PackedSequence(toBuffer, len(fromBuffer))
        packed[:] = fromBuffer

        return packed

    def unpack(self):
        return bytes(self)

class PackedCIGAR:
    __slots__ = "_buffer"

    def __init__(self, buffer: memoryview):
        self._buffer = buffer or bytearray()

    def __str__(self):
        seq = ""
        for c in self:
            seq += SEQUENCE_VALUES[c]
        return seq

    def __getitem__(self, i):
        if isinstance(i, slice):
            start = slice.start or 0
            stop = slice.stop or len(self)
            step = slice.step or 1
            return [self[a] for a in range(start, stop, step)]

        op = self._buffer[i]
        return (op >> 4, op & 0x1111)

    def __setitem__(self, i, value):
        if isinstance(i, slice):
            start = i.start or 0
            stop = i.stop or len(self)
            step = i.step or 1
            if hasattr(value, '__iter__') and len(value) == (stop - start) / step:
                #TODO optimise pairwise packing if step == 1
                for a, b in zip(range(start, stop, step), value):
                    if a % 2:
                        self._buffer[a / 2] = (self._buffer[a / 2] & 0b11110000) | b
                    else:
                        self._buffer[a / 2] = (self._buffer[a / 2] & 0b00001111) | (b << 4)
            else:
                #TODO make sequence mutable, this can only work for new files
                raise ValueError("Slice assignment can not change length of sequence.")
        else:
            self._buffer[i] = value[0] << 4 | value[1]

    def __iter__(self):
        def it():
            for b in self._buffer:
                yield (b >> 4, b & 0x1111)

        return it

    def __reversed__(self):
        def it():
            for b in reversed(self._buffer):
                yield (b >> 4, b & 0x1111)

        return it

    def __len__(self):
        return len(self._buffer)

    @staticmethod
    def pack(fromBuffer, toBuffer = None):
        if isinstance(fromBuffer, PackedCIGAR):
            # TODO memcpy to toBuffer
            return fromBuffer
        packed = PackedCIGAR(toBuffer)
        packed[:] = fromBuffer

        return packed

    def unpack(self):
        return list(self)

class TagHeader(C.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ("tag", C.c_char * 2),     # tag Two-character tag char[2]
        ("value_type", C.c_char),  # val_type Value type: AcCsSiIfZHB22,23 char
    ]

class Tag:
    __slots__ = '_header', '_buffer'
    def __init__(self, buffer: memoryview or None, offset: int = 0):
        if buffer:
            self._header = TagHeader.from_buffer(buffer, offset)
            if self._header.value_type == b'A':
                self._buffer = C.c_char.from_buffer(buffer, offset+3)
            elif self._header.value_type == b'i':
                self._buffer = C.c_int32.from_buffer(buffer, offset + 3)
            elif self._header.value_type == b'f':
                self._buffer = C.c_float.from_buffer(buffer, offset + 3)
            elif self._header.value_type in b'ZH':
                offset += 3
                start = offset
                while buffer[offset]: offset += 1
                self._buffer = (C.c_char * (offset - start)).from_buffer(buffer, start)
            elif self._header.value_type == b'B':
                array_type = C.c_char.from_buffer(buffer, offset + 3)
                length = C.c_uint32.from_buffer(buffer, offset + 4)
                self._buffer = (BTAG_TYPES[array_type] * length).from_buffer(buffer, offset + 8)
            else:
                raise InvalidBAM("Unknown tag value type.")
        else:
            self._header = TagHeader()
            self._buffer = None

    def size(self):
        if self._buffer is not None:
            length = 3
            if self._header.val_type == b'B':
                length += 5 + len(self._buffer)
            elif self._header.val_type == b'H':
                length += 4 + len(self._buffer)
            elif self._header.val_type == b'Z':
                length += 4 + len(self._buffer)
            return length
        else:
            return 0

    def __len__(self):
        if self._buffer is not None:
            if self._header.val_type in b'ZBH':
                return len(self._buffer)
            else:
                return 1
        else:
            return 0

    def __str__(self):
        return "".join("{}".format(b) for b in self)

    def __getitem__(self, i):
        if self._buffer:
            if self._header.val_type in b'ZBH':
                return self._buffer[i]
            else:
                return self._buffer
        else:
            raise ValueError("Buffer not initialised.")

    def __setitem__(self, i, value):
        if self._buffer:
            if self._header.val_type in b'ZBH':
                self._buffer[i] = value
            else:
                self._buffer = value
        else:
            raise ValueError("Buffer not initialised.")

    def __iter__(self):
        return iter(self._buffer)

    def __reversed__(self):
        return reversed(self._buffer)