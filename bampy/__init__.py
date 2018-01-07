import ctypes as C

import bampy.bam, bampy.bgzf
from bampy.bam import SEQUENCE_VALUES, OP_CODES

# if read only access_copy
# if write access_write
#path = os.path.expanduser(path)
#fh = open(path, mode + 'b')
#return mmap.mmap(fh.fileno(), 1000 if 'w' in mode else 0, access=mmap.ACCESS_WRITE if fh.writeable() else mmap.ACCESS_COPY)

class Record:
    def __init__(self, header = bam.RecordHeader(), name = "*", cigar = [], sequence = bytearray(), quality_scores = bytearray(), tags = bytearray(), reference = None):
        self._header = header
        #TODO init header to defaults
        self.name = name
        self.cigar = cigar
        self.sequence = sequence
        self.quality_scores = quality_scores
        self.tags = tags
        self.reference = reference

    @staticmethod
    def fromBuffer(buffer, offset = 0, references = []):
        buffer = memoryview(buffer)
        start = offset
        header = bam.RecordHeader.from_buffer(buffer, offset)
        offset += C.sizeof(bam.RecordHeader)
        name = (C.c_char * header.name_length).from_buffer(buffer, offset)
        offset += header.name_length
        cigar = bam.PackedCIGAR(buffer[offset: offset + header.cigar_length])
        offset += header.cigar_length
        array_len = (header.sequence_length + 1) // 2
        sequence = bam.PackedSequence(buffer[offset: offset + array_len], header.sequence_length)
        offset += array_len
        quality_scores = buffer[offset: offset + header.sequence_length]
        offset += header.sequence_length
        if offset < start + header.block_size:
            tags = buffer[offset:start + header.block_size]
        else:
            tags = bytearray()
        if header.reference_id == -1:
            reference = None
        else:
            reference = references[header.reference_id]
        return Record(header, name, cigar, sequence, quality_scores, tags, reference)

    @staticmethod
    def fromStream(stream):
        #TODO
        return Record()

    def __getattribute__(self, item):
        if item == "tags" and not isinstance(self.tags, dict):
            #TODO This could be optimised by assuming that the tags are sorted appropriately and only unpacking up to the requested tag
            offset = 0
            tags = {}
            while (offset < len(self.tags)):
                tag = bam.Tag(self.tags, offset)
                if tag.size():
                    self.tags[tag.tag] = tag
                else:
                    raise ValueError("Unexpected buffer size.")
                offset += tag.size()
            self.tags = tags
        return super().__getattribute__(item)

    def __getattr__(self, item):
        return getattr(self._header, item)

    def pack(self):
        self.sequence = bam.PackedSequence.pack(self.sequence)
        self.cigar = bam.PackedCIGAR.pack(self.cigar)

    def unpack(self):
        self.sequence = self.sequence.unpack()
        self.cigar = self.cigar.unpack()

