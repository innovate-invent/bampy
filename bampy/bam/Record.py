import ctypes as C
from enum import IntFlag
from .packed_sequence import PackedSequence
from .packed_cigar import PackedCIGAR
from .tag import Tag
from . import SEQUENCE_VALUES, OP_CODES, _qscore_to_str, _to_bytes, _to_str
from .. import sam
#TODO ascii -> __bytes__, __str__ -> __repr__, refactor everything, generators -> iterators

SIZEOF_INT32 = C.sizeof(C.c_uint32)

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

class Record:
    __slots__ = '_header', 'name', 'cigar', 'sequence', 'quality_scores', 'tags', 'reference', 'next_reference'
    def __init__(self, header = RecordHeader(), name = b"*", cigar = [], sequence = bytearray(), quality_scores = bytearray(), tags = bytearray(), references = None):
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
    def _data_from_buffer(header, buffer, offset = 0):
        start = offset
        # Name
        name = (C.c_char * header.name_length)
        name.__repr__ = _to_bytes
        name.__str__ = _to_str
        name = name.from_buffer(buffer, offset)
        offset += header.name_length
        if not header.name_length % 4: offset += 4 - header.name_length % 4  # Handle extra nulls
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
        quality_scores.__str__ = _qscore_to_str
        quality_scores = quality_scores.from_buffer(buffer, offset)
        offset += header.sequence_length
        # Tags
        if offset < start + header.block_size - C.sizeof(RecordHeader):
            tags = (C.c_ubyte * (header.block_size + C.sizeof(C.c_uint32) - offset + start - C.sizeof(RecordHeader))).from_buffer(buffer, offset)
        else:
            tags = {}
        return name, cigar, sequence, quality_scores, tags

    @staticmethod
    def from_buffer(buffer, offset = 0, references = []):
        buffer = memoryview(buffer)
        header = RecordHeader.from_buffer(buffer, offset)
        offset += C.sizeof(RecordHeader)
        return Record(header, *Record._data_from_buffer(header, buffer, offset), references)

    def to_buffer(self, buffer, offset):
        # TODO check if buffer large enough
        buffer = memoryview(buffer)
        self.pack()
        new = Record.__new__(Record)
        new._header = RecordHeader.from_buffer(buffer, offset)
        C.memmove(new._header , self._header, C.sizeof(RecordHeader))
        offset += C.sizeof(RecordHeader)
        new.name, new.cigar, new.sequence, new.quality_scores, new.tags = Record._data_from_buffer(new._header, buffer, offset)
        #TODO reduce to one memmove of entire block if contiguous
        C.memmove(new.name, self.name, len(self.name))
        C.memmove(new.cigar, self.cigar, len(self.cigar))
        C.memmove(new.sequence, self.sequence, len(self.sequence))
        C.memmove(new.quality_scores, self.quality_scores, len(self.quality_scores))
        C.memmove(new.tags, self.tags, len(self.tags))
        new.reference = self.reference
        new.next_reference = self.next_reference
        return new

    @staticmethod
    def from_stream(stream, references = []):
        header = bytearray(C.sizeof(RecordHeader))
        stream.readinto(header)
        header = RecordHeader.from_buffer(header)
        data = bytearray(header.block_size - C.sizeof(RecordHeader) + C.sizeof(C.c_int32))
        stream.readinto(data)
        return Record(header, *Record._data_from_buffer(header, data), references)

    def to_stream(self, stream):
        self.pack()
        stream.write(self._header)
        stream.write(self.name)
        stream.write(self.cigar)
        stream.write(self.sequence)
        stream.write(self.quality_scores)
        stream.write(self.tags)

    def __getattribute__(self, item):
        attr = super().__getattribute__(item)
        if item == "tags" and not isinstance(attr, dict):
            #TODO This could be optimised by assuming that the tags are sorted appropriately and only unpacking up to the requested tag
            offset = 0
            tags = {}
            while offset < len(attr):
                tag = Tag(attr, offset)
                if tag.size():
                    tags[tag.tag] = tag
                else:
                    raise ValueError("Unexpected buffer size.")
                offset += tag.size()
            self.tags = tags
            return tags

        return attr

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
        #TODO update header, pack tags?
        self.sequence = PackedSequence.pack(self.sequence)
        self.cigar = PackedCIGAR.pack(self.cigar)

    def unpack(self):
        self.sequence = self.sequence.unpack()
        self.cigar = self.cigar.unpack()

    @staticmethod
    def from_sam(line, references):
        name, flags, reference_name, position, mapping_quality, cigar, next_reference_name, next_position, template_length, sequence, quality_scores, *_tags = line.split(b"\t")
        flags = RecordFlags(int(flags))
        position = int(position) - 1
        mapping_quality = int(mapping_quality)
        next_position = int(next_position) - 1
        template_length = int(template_length)
        cigar = [(int(count), OP_CODES.index(op)) for count, op in sam.cigar_re.findall(cigar)]
        sequence = bytearray(SEQUENCE_VALUES.index(c) for c in sequence)
        quality_scores = bytearray(b - 33 for b in quality_scores)
        tags = {}
        for tag in _tags:
            tag = Tag.from_sam(tag)
            tags[tag.tag] = tag

        header = RecordHeader()
        #header.block_size = data[0]
        header.reference_id = next(i for i, ref in enumerate(references) if ref.name == reference_name)
        header.position = position
        header.name_length = len(name)
        header.mapping_quality = mapping_quality
        #header.bin = 0 #TODO
        header.cigar_length = len(cigar)
        header.flag = flags
        header.sequence_length = len(sequence)
        header.next_reference_id = next(i for i, ref in enumerate(references) if ref.name == next_reference_name)
        header.next_position = next_position
        header.template_length = template_length

        return Record(header, name, cigar, sequence, quality_scores, tags, references)

    def __repr__(self):
        return "{qname}\t{flag}\t{rname}\t{pos}\t{mapq}\t{cigar}\t{rnext}\t{pnext}\t{tlen}\t{seq}\t{qual}".format(
            qname = repr(self.name),
            flag = repr(self.flag),
            rname = repr(self.reference.name),
            pos = repr(self.position),
            mapq = repr(self.mapping_quality),
            cigar = repr(self.cigar),
            rnext = repr(self.next_reference.name),
            pnext = repr(self.next_position),
            tlen = repr(self.template_length),
            seq = repr(self.sequence),
            qual = repr(self.quality_scores),
        ) + (('\t' + '\t'.join(repr(tag) for tag in self.tags.values())) if len(self.tags) else '')

    def __bytes__(self):
        #TODO need to check if packed or unpacked
        return b"\t".join((
            self.name,
            str(self.flag).encode('ASCII'),
            self.reference.name.encode('ASCII'),
            str(self.position).encode('ASCII'),
            str(self.mapping_quality).encode('ASCII'),
            bytes(self.cigar),
            self.next_reference.name.encode('ASCII'),
            str(self.next_position).encode('ASCII'),
            str(self.template_length).encode('ASCII'),
            bytes(self.sequence),
            bytes(map(lambda x: x + 33, self.quality_scores)),
            b'\t'.join(bytes(tag) for tag in self.tags.values())
        ))

    def __len__(self):
        return self.block_size + SIZEOF_INT32

    def copy(self):
        new = Record.__new__(Record)
        new._header = RecordHeader.from_buffer_copy(self._header)
        new.name = bytearray(self.name)
        new.cigar = self.cigar.copy() if isinstance(self.cigar, PackedCIGAR) else bytearray(self.cigar)
        new.sequence = self.sequence.copy() if isinstance(self.sequence) else bytearray(self.sequence)
        new.quality_scores = bytearray(self.quality_scores)
        new.tags = {}
        for value in self.tags.values():
            tag = value.copy()
            new.tags[tag.tag] = tag
        new.reference = self.reference
        new.next_reference = self.next_reference
        return new