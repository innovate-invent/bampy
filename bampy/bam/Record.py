import ctypes as C
from enum import IntFlag

from .packed_cigar import OP_CODES, PackedCIGAR
from .packed_sequence import PackedSequence, SEQUENCE_VALUES
from .tag import Tag
from .util import BufferUnderflow, _qscore_to_str, _to_bytes, _to_str, alignment_length, reg2bin
from .. import sam

SIZEOF_UINT32 = C.sizeof(C.c_uint32)

SIZEOF_INT32 = C.sizeof(C.c_int32)

CSTRING_TERMINATOR = (C.c_ubyte * 1)()  # Represents null byte used to terminate the record name field


class RecordFlags(IntFlag):
    """
    Represents flag bit values. Can be OR'd (|) together or AND (&) to determine flag setting.
    """
    MULTISEG = 1 << 0  # template having multiple segments in sequencing
    ALIGNED = 1 << 1  # each segment properly aligned according to the aligner
    UNMAPPED = 1 << 2  # segment unmapped
    MATE_UNMAPPED = 1 << 3  # next segment in the template unmapped
    REVERSE_COMPLIMENTED = 1 << 4  # SEQ being reverse complemented
    MATE_REVERSED = 1 << 5  # SEQ of the next segment in the template being reversed
    READ1 = 1 << 6  # the first segment in the template
    READ2 = 1 << 7  # the last segment in the template
    SECONDARY = 1 << 8  # secondary alignment
    QCFAIL = 1 << 9  # not passing quality controls
    DUPLICATE = 1 << 10  # PCR or optical duplicate


class RecordHeader(C.LittleEndianStructure):
    """
    Represents a BAM record header in memory
    """
    _pack_ = 1
    _fields_ = [
        ("block_size", C.c_int32),  # block_size Length of the remainder of the alignment record int32 t
        ("reference_id", C.c_int32),  # refID Reference sequence ID, −1 ≤ refID < n ref; -1 for a read without a mapping position. int32 t [-1]
        ("position", C.c_int32),  # pos 0-based leftmost coordinate (= POS − 1) int32 t [-1]
        ("name_length", C.c_uint8),
        # bin mq nl bin<<16|MAPQ<<8|l read name; bin is computed from the mapping position;18 l read name is the length of read name below (= length(QNAME) + 1). uint32 t
        ("mapping_quality", C.c_uint8),
        # bin mq nl bin<<16|MAPQ<<8|l read name; bin is computed from the mapping position;18 l read name is the length of read name below (= length(QNAME) + 1). uint32 t
        ("bin", C.c_uint16),
        # bin mq nl bin<<16|MAPQ<<8|l read name; bin is computed from the mapping position;18 l read name is the length of read name below (= length(QNAME) + 1). uint32 t
        ("cigar_length", C.c_uint16),  # flag nc FLAG<<16|n_cigar op; n_cigar op is the number of operations in CIGAR. uint32 t
        ("flag", C.c_uint16),  # flag nc FLAG<<16|n_cigar op; n_cigar op is the number of operations in CIGAR. uint32 t
        ("sequence_length", C.c_int32),  # l_seq Length of SEQ int32 t
        ("next_reference_id", C.c_int32),  # next_refID Ref-ID of the next segment (−1 ≤ mate refID < n ref) int32 t [-1]
        ("next_position", C.c_int32),  # next_pos 0-based leftmost pos of the next segment (= PNEXT − 1) int32 t [-1]
        ("template_length", C.c_int32),  # tlen Template length (= TLEN) int32 t [0]
    ]

    def __len__(self):
        return SIZEOF_RECORDHEADER


SIZEOF_RECORDHEADER = C.sizeof(RecordHeader)


class Record:
    """
    Represents and manages record data in memory.
    Record data is not necessarily stored in BAM format in memory.
    """
    __slots__ = '_header', '_name', '_cigar', '_sequence', '_quality_scores', '_tags', '_reference', '_next_reference', '_buffer', '_tags_offset'

    def __init__(self, header=RecordHeader(), name=b"*", cigar=[], sequence=bytearray(), quality_scores=bytearray(), tags=bytearray(),
                 references=None, _buffer=None):
        self._header = header
        # TODO init header to defaults
        self._name = name
        self._cigar = cigar
        self._sequence = sequence
        self._quality_scores = quality_scores
        self._tags = tags
        self._tags_offset = 0
        self._reference = None if not references or header.reference_id == -1 else references[header.reference_id]
        self._next_reference = None if not references or header.next_reference_id == -1 else references[header.next_reference_id]
        self._buffer = _buffer
    
    # --- Property getters and setters ---
    @property
    def name(self):
        if self._name is None:
            self._data_from_buffer()
        return self._name

    @name.setter
    def name(self, value):
        self._header.block_size += len(value) - len(self._name)
        self._name = value
        self._header.name_length = len(value) + 1

    @property
    def cigar(self):
        if self._cigar is None:
            self._data_from_buffer()
        return self._cigar

    @cigar.setter
    def cigar(self, value):
        self._header.block_size += len(value) - len(self._value)
        # TODO update template length?
        self._cigar = value
        self._header.cigar_length = len(value)
        self._update_bin()

    @property
    def sequence(self):
        if self._sequence is None:
            self._data_from_buffer()
        return self._sequence

    @sequence.setter
    def sequence(self, value):
        self._header.block_size += len(value) - len(self._sequence)
        self._sequence = value
        self._header.sequence_length = len(value)

    @property
    def quality_scores(self):
        if self._quality_scores is None:
            self._data_from_buffer()
        return self._quality_scores

    @quality_scores.setter
    def quality_scores(self, value):
        self._header.block_size += len(value) - len(self._quality_scores)
        self._quality_scores = value

    def _unpack_tags(self):
        """
        Helper to unpack tag data from the end of the Record
        """
        if not self._tags_offset:
            self._data_from_buffer()
        # Parse tag data
        tags = []
        buffer = self._buffer
        offset = self._tags_offset
        buffer_len = len(buffer)
        while 0 < offset < buffer_len:
            tag = Tag(buffer, offset)
            size = tag.size()
            if size:
                tags.append(tag)
            else:
                raise ValueError("Unexpected buffer size.")
            offset += size
        self._tags = tags

    def get_tag(self, name):
        if self._tags is None:
            self._unpack_tags()

        for tag in self._tags:
            if tag.tag == name:
                return tag
        else:
            raise IndexError("{} tag not defined.".format(name))

    def set_tag(self, value):
        if self._tags is None:
            self._unpack_tags()
        size = value.size()
        for i, tag in enumerate(self._tags):
            if tag.tag == value.tag:
                self._tags[i] = value
                self._header.block_size += size - tag.size()
                return
        else:
            self._tags.append(value)
            self._header.block_size += size

    def del_tag(self, name):
        if self._tags is None:
            self._unpack_tags()
        for i, tag in enumerate(self._tags):
            if tag.tag == name:
                self._tags.pop(i)
                size = tag.size()
                self._header.block_size -= size

    @property
    def position(self):
        return self._header.block_size

    @position.setter
    def position(self, value):
        self._header.position = value
        self._update_bin()

    @property
    def reference(self):
        return self._reference

    @reference.setter
    def reference(self, value):
        self._reference = value
        self._header.reference_id = value.index

    @property
    def next_reference(self):
        return self._next_reference

    @next_reference.setter
    def next_reference(self, value):
        self._next_reference = value
        self._header.next_reference_id = value.index

    @property
    def bin(self):
        return self._header.bin

    @bin.setter
    def bin(self, value):
        self._header.bin = value

    @property
    def mapping_quality(self):
        return self._header.mapping_quality

    @mapping_quality.setter
    def mapping_quality(self, value):
        self._header.mapping_quality = value

    @property
    def flags(self):
        return RecordFlags(self._header.flag)

    @flags.setter
    def flags(self, value):
        self._header.flag = value

    @property
    def next_position(self):
        return self._header.next_position

    @next_position.setter
    def next_position(self, value):
        self._header.next_position = value

    @property
    def template_length(self):
        return self._header.template_length

    @template_length.setter
    def template_length(self, value):
        self._header.template_length = value

    def _data_from_buffer(self) -> None:
        """
        Maps out the name, cigar, sequence, quality scores, and tag data in memory.
        :param header: The RecordHeader instance that describes the data.
        :param buffer: The buffer to map.
        :param offset: The offset into the buffer that should point at the first byte of the name data.
        :return: Tuple containing in order the name, cigar, sequence, quality scores, and tags.
        """
        header = self._header
        buffer = self._buffer
        assert buffer, "No provided record data."
        offset = 0
        # Name
        name = (C.c_char * (header.name_length - 1))  # Exclude Null
        name.__bytes__ = _to_bytes
        name.__repr__ = _to_str
        name = name.from_buffer(self._buffer, offset)
        offset += header.name_length
        # Cigar
        cigar = (C.c_uint32 * header.cigar_length).from_buffer(buffer, offset)
        cigar = PackedCIGAR(cigar)
        offset += header.cigar_length * SIZEOF_UINT32
        # Sequence
        array_len = (header.sequence_length + 1) // 2
        sequence = (C.c_ubyte * array_len).from_buffer(buffer, offset)
        sequence = PackedSequence(sequence, header.sequence_length)
        offset += array_len
        # Quality Scores
        quality_scores = (C.c_ubyte * header.sequence_length)
        quality_scores.__repr__ = _qscore_to_str
        quality_scores = quality_scores.from_buffer(buffer, offset)
        offset += header.sequence_length
        # Tags
        if offset < header.block_size - SIZEOF_RECORDHEADER:
            tags = None
            self._tags_offset = offset
            # tags = (C.c_ubyte * (header.block_size + SIZEOF_UINT32 - offset - SIZEOF_RECORDHEADER)).from_buffer(buffer, offset)
        else:
            tags = bytes()
        self._name, self._cigar, self._sequence, self._quality_scores, self._tags = name, cigar, sequence, quality_scores, tags
        self._buffer = None

    @staticmethod
    def from_buffer(buffer, offset=0, references=[]) -> 'Record':
        """
        Maps record data from a buffer object.
        :param buffer: The buffer to map into.
        :param offset: The offset into the buffer pointing at the first byte of the record.
        :param references: A list of Reference objects to dereference the record reference id.
        :return: An instance of Record.
        """
        buffer = memoryview(buffer)
        try:
            header = RecordHeader.from_buffer(buffer, offset)
            offset += SIZEOF_RECORDHEADER
            return Record(header, None, None, None, None, None, references, buffer[offset:])
        except ValueError:
            raise BufferUnderflow()

    def to_buffer(self, buffer, offset) -> 'Record':
        """
        Copies the Record instance into a buffer object.
        :param buffer: The buffer to copy into.
        :param offset: The offset into the buffer pointing where the first byte will be written.
        :return: An instance of the new record in buffer.
        """
        # TODO check if buffer large enough
        buffer = memoryview(buffer)
        self.pack()
        new = Record.__new__(Record)
        buffer_ptr = C.addressof(buffer)
        new._header = RecordHeader.from_buffer(buffer, offset)
        C.memmove(buffer_ptr, C.addressof(self._header), SIZEOF_RECORDHEADER)
        buffer_ptr += SIZEOF_RECORDHEADER
        # TODO reduce to one memmove of entire block if contiguous (how to determine if contiguous?)
        length = len(self.name)
        C.memmove(buffer_ptr, self.name, length)  # Buffer initialised to null so will have terminating null after copy
        buffer_ptr += length
        length = len(self.cigar.buffer)
        C.memmove(buffer_ptr, self.cigar.buffer, length)
        buffer_ptr += length
        length = len(self.sequence.buffer)
        C.memmove(buffer_ptr, self.sequence.buffer, length)
        buffer_ptr += length
        length = len(self.quality_scores)
        C.memmove(buffer_ptr, self.quality_scores, length)
        buffer_ptr += length
        C.memmove(buffer_ptr, self._tags, len(self._tags))
        new.reference = self.reference
        new.next_reference = self.next_reference
        return new

    @staticmethod
    def from_stream(stream, references=[]) -> 'Record':
        """
        Copies record data from the stream into memory.
        :param stream: The stream instance to read from (must have readinto() function).
        :param references: A list of Reference objects to dereference the record reference id.
        :return: An instance of the read record.
        """
        header = bytearray(SIZEOF_RECORDHEADER)
        if stream.readinto(header) != SIZEOF_RECORDHEADER:
            raise EOFError()
        header = RecordHeader.from_buffer(header)
        data_len = header.block_size - SIZEOF_RECORDHEADER + SIZEOF_INT32
        data = bytearray(data_len)
        assert stream.readinto(data) == data_len, "Unexpected data length."
        return Record(header, None, None, None, None, None, references, data)

    def to_stream(self, stream) -> None:
        """
        Copies the record into the stream object.
        :param stream: The stream instance to read from (must have write() function).
        :return: None
        """
        data = self.pack()
        for datum in data:
            stream.write(datum)

    def _update_bin(self) -> None:
        """
        Updates the bin field in the record header.
        Calculates the bin by calling reg2bin.
        :return: None
        """
        self._header.bin = reg2bin(self._header.position, self._header.position + alignment_length(self.cigar))

    def pack(self, update=False) -> [RecordHeader, C.Array, C.Array, bytearray, bytearray, bytearray, bytearray]:
        """
        Prepares the record to be written as BAM format.
        Converts sequence and cigar to PackedSequence, PackedCIGAR.
        Packs all tags into byte array.
        :param update: Set to True to call update().
        :return: List containing in order: record header, name, name null terminator, cigar buffer, sequence buffer, quality scores, tags.
        """
        if not self._name:
            self._data_from_buffer()
        self._sequence = PackedSequence.pack(self._sequence)
        self._cigar = PackedCIGAR.pack(self._cigar)
        tag_buffer = bytearray()
        if self._tags:
            for tag in self._tags:
                tag_buffer += tag.pack()
        return [self._header, self._name, CSTRING_TERMINATOR, self._cigar.buffer, self._sequence.buffer, self._quality_scores, tag_buffer]

    def unpack(self) -> None:
        """
        Unpack record data for faster access.
        See PackedCIGAR.unpack() and PackedSequence.unpack().
        :return: None
        """
        self._sequence = self._sequence.unpack()
        self._cigar = self._cigar.unpack()
        # TODO tags?

    @staticmethod
    def from_sam(line, references) -> 'Record':
        """
        Parse SAM record into memory.
        :param line: A bytes like object containing the record data (Must have split() function).
        :param references: A list of Reference objects to dereference the record reference id.
        :return: A Record instance representing the record data.
        """
        name, flags, reference_name, position, mapping_quality, cigar, next_reference_name, next_position, template_length, sequence, quality_scores, *_tags = line.split(
            b"\t")
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
        # header.block_size = data[0]
        header.reference_id = next(i for i, ref in enumerate(references) if ref.name == reference_name)
        header.position = position
        header.name_length = len(name)
        header.mapping_quality = mapping_quality
        header.bin = reg2bin(position, position + alignment_length(cigar))
        header.cigar_length = len(cigar)
        header.flag = flags
        header.sequence_length = len(sequence)
        header.next_reference_id = next(i for i, ref in enumerate(references) if ref.name == next_reference_name)
        header.next_position = next_position
        header.template_length = template_length

        return Record(header, name, cigar, sequence, quality_scores, tags, references)

    def __repr__(self) -> str:
        """
        Returns a string representation of the record data.
        For debugging use only, see __bytes__() to convert to SAM.
        :return: String representing record.
        """
        return "{qname}\t{flag}\t{rname}\t{pos}\t{mapq}\t{cigar}\t{rnext}\t{pnext}\t{tlen}\t{seq}\t{qual}".format(
            qname=self.name,
            flag=self.flag,
            rname=self.reference.name,
            pos=self.position,
            mapq=self.mapping_quality,
            cigar=repr(self.cigar),
            rnext=self.next_reference.name,
            pnext=self.next_position,
            tlen=self.template_length,
            seq=repr(self.sequence),
            qual=self.quality_scores,
        ) + (('\t' + '\t'.join(repr(tag) for tag in self.tags.values())) if len(self.tags) else '')

    def __bytes__(self) -> bytes:
        """
        Converts record to SAM format.
        :return: A bytes object representing record data in SAM format.
        """
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

    def __len__(self) -> int:
        """
        Returns the bytes length of the record.
        :return: The byte length of the record in memory
        """
        return self.block_size + SIZEOF_INT32

    def copy(self) -> 'Record':
        """
        Copy the record in memory.
        :return: A new instance of Record with the copied data.
        """
        new = Record.__new__(Record)
        new._header = RecordHeader.from_buffer_copy(self._header)
        new._name = bytearray(self.name)
        new._cigar = self.cigar.copy() if isinstance(self.cigar, PackedCIGAR) else bytearray(self.cigar)
        new._sequence = self.sequence.copy() if isinstance(self.sequence) else bytearray(self.sequence)
        new._quality_scores = bytearray(self.quality_scores)
        tags = self._tags
        if tags:
            new.tags = self._tags[:]
        else:
            new._tags = None
        new._reference = self.reference
        new._next_reference = self.next_reference
        return new
