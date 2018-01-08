import ctypes as C
from enum import IntFlag

SEQUENCE_VALUES = tuple("=ACMGRSVTWYHKDBN")
OP_CODES = tuple("MIDNSHP=X")
BTAG_TYPES = {b'c':C.c_int8, b'C':C.c_uint8, b's':C.c_int16, b'S':C.c_uint16, b'i':C.c_int32, b'I':C.c_uint32, b'f':C.c_float}
TAG_TYPES = {b'A':C.c_char}
TAG_TYPES.update(BTAG_TYPES)

class RecordFlags(IntFlag):
    MULTISEG                = 1 << 0    # template having multiple segments in sequencing
    ALIGNED                 = 1 << 1    # each segment properly aligned according to the aligner
    UNMAPPED                = 1 << 2    # segment unmapped
    MATE_UNMAPPED           = 1 << 3    # next segment in the template unmapped
    REVERSE_COMPLIMENTED    = 1 << 4    # SEQ being reverse complemented
    MATE_REVERSED           = 1 << 5    # SEQ of the next segment in the template being reversed
    READ1                   = 1 << 6    # the first segment in the template
    READ2                   = 1 << 7    # the last segment in the template
    SECONDARY               = 1 << 8    # secondary alignment
    QCFAIL                  = 1 << 9    # not passing quality controls
    DUPLICATE               = 1 << 10   # PCR or optical duplicate

def isBAM(buffer):
    return buffer[:4] == b'BAM\x01'

class InvalidBAM(ValueError):
    pass

class Reference:
    __slots__ = 'name', 'length'
    def __init__(self, name: str, length: int):
        self.name = name #type: str
        self.length = length #type: int

    def __str__(self):
        return "SN:{} LN:{}".format(self.name, self.length)

def header_from_stream(stream, _magic = None):
    # Provide a friendly way of peeking into a stream for data type discovery
    if not _magic:
        magic = bytearray(4)
        stream.readinto(magic)
    if not isBAM(_magic or magic):
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
        refs.append(Reference(name.decode('ASCII'), seq_length.value))
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
        refs.append(Reference(toStr(name), seq_length.value))
    return header, refs, offset

class RecordHeader(C.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ("block_size", C.c_int32),         # block_size Length of the remainder of the alignment record int32 t
        ("reference_id", C.c_int32),       # refID Reference sequence ID, −1 ≤ refID < n ref; -1 for a read without a mapping position. int32 t [-1]
        ("position", C.c_int32),           # pos 0-based leftmost coordinate (= POS − 1) int32 t [-1]
        ("name_length", C.c_uint8),         # bin mq nl bin<<16|MAPQ<<8|l read name; bin is computed from the mapping position;18 l read name is the length of read name below (= length(QNAME) + 1). uint32 t
        ("mapping_quality", C.c_uint8),     # bin mq nl bin<<16|MAPQ<<8|l read name; bin is computed from the mapping position;18 l read name is the length of read name below (= length(QNAME) + 1). uint32 t
        ("bin", C.c_uint16),                # bin mq nl bin<<16|MAPQ<<8|l read name; bin is computed from the mapping position;18 l read name is the length of read name below (= length(QNAME) + 1). uint32 t
        ("cigar_length", C.c_uint16),       # flag nc FLAG<<16|n_cigar op; n_cigar op is the number of operations in CIGAR. uint32 t
        ("flag", C.c_uint16),               # flag nc FLAG<<16|n_cigar op; n_cigar op is the number of operations in CIGAR. uint32 t
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
            self._buffer = bytes((length + 1) // 2)

        self._length = length

    def __str__(self):
        return "".join(SEQUENCE_VALUES[c] for c in self)

    def __repr__(self):
        return self.__str__()

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
            return self._buffer[i//2] & 0b00001111
        else:
            return self._buffer[i//2] >> 4

    def __setitem__(self, i, value):
        if isinstance(i, slice):
            start = i.start or 0
            stop = i.stop or len(self)
            step = i.step or 1
            if hasattr(value, '__iter__') and len(value) == (stop - start) / step:
                #TODO optimise pairwise packing if step == 1
                for a, b in zip(range(start, stop, step), value):
                    if a % 2:
                        self._buffer[a // 2] = (self._buffer[a // 2] & 0b11110000) | b
                    else:
                        self._buffer[a // 2] = (self._buffer[a // 2] & 0b00001111) | (b << 4)
            else:
                #TODO make sequence mutable, this can only work for new files
                raise ValueError("Slice assignment can not change length of sequence.")
        else:
            if i % 2:
                self._buffer[i // 2] = (self._buffer[i // 2] & 0b11110000) | value
            else:
                self._buffer[i // 2] = (self._buffer[i // 2] & 0b00001111) | (value << 4)

    def __iter__(self):
        count = 1
        for b in self._buffer:
            yield b >> 4
            if count < self._length:
                yield b & 0b00001111
            count += 2

    def __reversed__(self):
        odd = self._length & 1
        for b in reversed(self._buffer):
            if odd:
                odd = False
            else:
                yield b & 0b00001111
            yield b >> 4

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

def QScoreToStr(data):
    return ''.join(map(lambda x:chr(x+33), data))

def toBytes(data):
    return data.value

def toStr(data):
    if isinstance(data.value, bytes):
        return data.value.decode('ASCII')
    else:
        return str(data.value)

class PackedCIGAR:
    __slots__ = "_buffer"

    def __init__(self, buffer: memoryview):
        self._buffer = buffer or bytearray()

    def __str__(self):
        return "".join("{}{}".format(c[0], OP_CODES[c[1]]) for c in self)

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, i):
        if isinstance(i, slice):
            start = slice.start or 0
            stop = slice.stop or len(self)
            step = slice.step or 1
            return [self[a] for a in range(start, stop, step)]

        op = self._buffer[i]
        return (op >> 4, op & 0b1111)

    def __setitem__(self, i, value):
        if isinstance(i, slice):
            start = i.start or 0
            stop = i.stop or len(self)
            step = i.step or 1
            if hasattr(value, '__iter__') and len(value) == (stop - start) / step:
                for a, b in zip(range(start, stop, step), value):
                    self._buffer[a] = C.c_uint32.__ctype_le__(b)
            else:
                #TODO make cigar mutable, this can only work for new files
                raise ValueError("Slice assignment can not change length of sequence.")
        else:
            self._buffer[i] = value[0] << 4 | value[1]

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
            offset += C.sizeof(TagHeader)
            if self._header.value_type in TAG_TYPES:
                self._buffer = TAG_TYPES[self._header.value_type].from_buffer(buffer, offset)
            elif self._header.value_type in b'ZH':
                start = offset
                while buffer[offset]: offset += 1
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

    def __str__(self):
        return "{}:{}:{}".format(self._header.tag.decode('ASCII'), self._header.value_type.decode('ASCII') if self._header.value_type in b'AifZHB' else 'i', toStr(self._buffer))

    def __getattr__(self, item):
        return getattr(self._header, item)

    def __getitem__(self, i):
        if self._buffer:
            if self._header.value_type in b'ZBH':
                return self._buffer[i]
            else:
                return self._buffer
        else:
            raise ValueError("Buffer not initialised.")

    def __setitem__(self, i, value):
        if self._buffer:
            if self._header.value_type in b'ZBH':
                self._buffer[i] = value
            else:
                self._buffer = value
        else:
            raise ValueError("Buffer not initialised.")

class Record:
    def __init__(self, header = RecordHeader(), name = "*", cigar = [], sequence = bytearray(), quality_scores = bytearray(), tags = bytearray(), references = None):
        self._header = header
        #TODO init header to defaults
        self.name = name
        self.cigar = cigar
        self.sequence = sequence
        self.quality_scores = quality_scores
        self.tags = tags
        self.reference = None if not references or header.reference_id == -1 else references[header.reference_id]
        self.next_reference = None if not references or header.next_reference_id == -1 else references[header.next_reference_id]

    @staticmethod
    def fromBuffer(buffer, offset = 0, references = []):
        buffer = memoryview(buffer)
        start = offset
        header = RecordHeader.from_buffer(buffer, offset)
        offset += C.sizeof(RecordHeader)
        # Name
        name = (C.c_char * header.name_length)
        name.__repr__ = toBytes
        name.__str__ = toStr
        name = name.from_buffer(buffer, offset)
        offset += header.name_length
        if not header.name_length % 4: offset += 4 - header.name_length % 4 # Handle extra nulls
        # Cigar
        cigar = (C.c_uint32.__ctype_le__ * header.cigar_length).from_buffer(buffer, offset)
        cigar = PackedCIGAR(cigar)
        offset += header.cigar_length * C.sizeof(C.c_uint32)
        # Sequence
        array_len = (header.sequence_length + 1) // 2
        sequence = (C.c_ubyte * array_len).from_buffer(buffer, offset)
        sequence = PackedSequence(sequence, header.sequence_length)
        offset += array_len
        # Quality Scores
        quality_scores = (C.c_ubyte * header.sequence_length)
        quality_scores.__str__ = QScoreToStr
        quality_scores = quality_scores.from_buffer(buffer, offset)
        offset += header.sequence_length
        # Tags
        if offset < start + header.block_size:
            tags = (C.c_ubyte * (header.block_size + C.sizeof(C.c_uint32) - offset + start)).from_buffer(buffer, offset)
        else:
            tags = {}
        return Record(header, name, cigar, sequence, quality_scores, tags, references)

    @staticmethod
    def fromStream(stream):
        #TODO
        return Record()

    def __getattribute__(self, item):
        if item == "tags":
            tags_buffer = super().__getattribute__(item)
            if not isinstance(tags_buffer, dict):
                #TODO This could be optimised by assuming that the tags are sorted appropriately and only unpacking up to the requested tag
                offset = 0
                tags = {}
                while offset < len(tags_buffer):
                    tag = Tag(tags_buffer, offset)
                    if tag.size():
                        tags[tag.tag] = tag
                    else:
                        raise ValueError("Unexpected buffer size.")
                    offset += tag.size()
                self.tags = tags
                return tags
            return tags_buffer
        return super().__getattribute__(item)

    def __getattr__(self, item):
        try:
            return getattr(self._header, item)
        except AttributeError:
            if item == 'flags':
                self.flags = RecordFlags(self._header.flag)
                return self.flags
            else:
                raise

    def pack(self):
        self.sequence = PackedSequence.pack(self.sequence)
        self.cigar = PackedCIGAR.pack(self.cigar)

    def unpack(self):
        self.sequence = self.sequence.unpack()
        self.cigar = self.cigar.unpack()

    def __str__(self):
        return "{qname}\t{flag}\t{rname}\t{pos}\t{mapq}\t{cigar}\t{rnext}\t{pnext}\t{tlen}\t{seq}\t{qual}".format(
            qname = self.name,
            flag = self.flag,
            rname = self.reference.name,
            pos = self.position,
            mapq = self.mapping_quality,
            cigar = self.cigar,
            rnext = self.next_reference.name,
            pnext = self.next_position,
            tlen = self.template_length,
            seq = self.sequence,
            qual = self.quality_scores,
        ) + (('\t' + '\t'.join(str(tag) for tag in self.tags.values())) if len(self.tags) else '')

    def __len__(self):
        return self.block_size + C.sizeof(C.c_uint32)