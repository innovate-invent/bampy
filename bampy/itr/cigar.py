import re
from ..bam.util import CONSUMES_REFERENCE, CONSUMES_QUERY, CLIPPED, CigarOps

MDOps = re.compile('(\d*)\^?([A-Za-z])')


class CigarIterator:
    __slots__ = 'record', 'ops', 'md', 'opsI', 'op_pos', 'op_start', 'seq_pos', 'ref_start', 'ref_pos', 'mdI'

    def __init__(self, record: '..bam.Record'):
        self.record = record
        self.ops = record.cigar or ()  # List of CIGAR operations
        self.md = None  # Reference bases from MD tag
        self.rewind()

    def _buildMD(self):
        try:
            md = self.record.tags.get(b'MD')
            if md:
                self.md = []
                i = 0
                pos = 0
                mdOffset = 0
                # Count number of clipped bases
                for count, op in self.ops:
                    if op == CigarOps.SOFT_CLIP:
                        mdOffset += count
                    elif op != CigarOps.HARD_CLIP:
                        break
                for mdOp in MDOps.finditer(md):
                    matchCount, refBase = mdOp.group(1, 2)
                    mdOffset += int(matchCount or 0)
                    while pos <= mdOffset: # Scan CIGAR for insertions and add to offset as MD does not include insertions in MD coordinate space
                        pos += self.ops[i][0]
                        if self.ops[i][1] == CigarOps.INS:
                            mdOffset += self.ops[i][0]
                        i += 1
                    self.md.append((refBase, mdOffset))
                    mdOffset += 1
        except BaseException as e:
            raise RuntimeError("Unhandled exception while parsing MD Tag.") from e

    def _getMD(self) -> tuple:
        if self.md == None:
            self._buildMD()
        if self.md == None or self.mdI >= len(self.md):
            return (None, None)
        while self.md[self.mdI][1] < self.op_pos:
            self.mdI += 1
            if self.mdI >= len(self.md):
                return (None, None)
        return self.md[self.mdI]

    def rewind(self):
        self.opsI = 0  # Current index in ops
        self.op_pos = 0  # Current position in operation including all previous operations
        self.op_start = 0  # Start of current operation
        self.seq_pos = 0  # Current sequence position
        self.ref_start = self.record.position  # Aligned starting position of unclipped sequence
        self.ref_pos = self.ref_start  # Current reference position
        self.mdI = 0  # Current index in MD

    def __iter__(self):
        self.rewind()
        yield self
        while self.next():
            yield self

    def step(self, i: int):
        #TODO support negative step
        if i < 0: raise NotImplementedError("Negative stepping not yet supported.")
        return self.skip_to_pos(self.op_pos + i)

    def step_op(self) -> int:
        if not self.valid: return 0
        dist = self.ops[self.opsI][0] - (self.op_pos - self.op_start)
        self.op_start += self.op_length
        self.op_pos = self.op_start
        if self.in_seq:
            self.seq_pos += dist
        if self.in_ref:
            self.ref_pos += dist
        self.opsI += 1
        return dist

    def next(self) -> bool:
        return self.step(1)

    #def prev(self) -> bool:
    #    return self.step(-1)

    def next_op(self) -> bool:
        if not self.valid or not len(self.ops):
            return False
        self.step_op()
        return self.valid

    def skip_clipped(self, hardOnly: bool = False) -> int:
        if not self.valid: return 0
        count = 0
        if hardOnly:
            if self.op == CigarOps.HARD_CLIP:
                return self.step_op()
            else:
                return 0
        while self.valid and self.clipped:
            count += self.step_op()
        return count

    def skip_to_pos(self, pos: int): # Pos is in cigar space
        if pos < 0:
            raise IndexError
        if not self.valid:
            return False

        #Jog through operations until new position
        while self.op_end < pos: self.step_op()
        delta = pos - self.op_pos

        if delta:
            #Add remainder within current operation
            self.op_pos = pos
            if self.in_seq:
                self.seq_pos += delta
            if self.in_ref:
                self.ref_pos += delta

        return self.valid

    def skip_to_ref_pos(self, pos): # Pos is in reference space
        #Jog to op that contains pos
        while self.valid and (self.ref_pos + self.op_length < pos or not self.in_ref):
            self.step_op()

        #Step within current op
        self.step(pos - self.ref_pos)

        return self.valid

    def skip_to_nonref(self) -> bool: # Move iterator to next non-reference cigar position (variant in MD tag)
        md = self._getMD()
        if md[0] is None:
            return False
        if md[1] == self.op_pos:
            self.mdI += 1
        md = self._getMD()
        if md[0] is None or not self.valid:
            return False
        return self.skip_to_ref_pos(md[1])

    @property
    def valid(self):
        return self.opsI < len(self.ops) and (self.opsI + 1 != len(self.ops) or self.op_pos <= self.op_end)

    @property
    def op_length(self) -> int:
        return self.ops[self.opsI][0] if self.opsI < len(self.ops) else 0

    @property
    def op_end(self) -> int:
        l = self.op_length
        if l == 0:
            return self.op_start
        else:
            return self.op_start + l - 1

    @property
    def op_remaining(self) -> int:
        return self.op_length - (self.op_pos - self.op_start)

    @property
    def in_ref(self) -> bool: # Returns true if the passed operation has a reference coordinate
        return CONSUMES_REFERENCE[self.op]

    @property
    def in_seq(self) -> bool: # Returns true if the passed operation has a sequence coordinate
        return CONSUMES_QUERY[self.op]

    @property
    def clipped(self) -> bool:
        return CLIPPED[self.op]

    @property
    def ref_base(self) -> int or None:
        return (self.seq_base if self.matches_ref else self._getMD()[0]) if self.in_ref else None

    @property
    def matches_ref(self) -> bool:
        return self._getMD()[1] != self.op_pos #self.getSeqBase() == self.getRefBase()

    @property
    def seq_base(self) -> int or None:
        return self.record.sequence[self.seq_pos] if self.in_seq else None

    @seq_base.setter
    def set_seq_base(self, int) -> bool:
        if self.in_seq:
            self.record.sequence[self.seq_pos] = int
        else:
            return False
        return True

    @property
    def base_qual(self) -> int or None:
        return self.record.quality_scores[self.seq_pos] if self.in_seq else None

    @base_qual.setter
    def set_base_qual(self, qual: int) -> bool:
        if self.in_seq:
            self.record.quality_scores[self.seq_pos] = qual
        else:
            return False
        return True

    @property
    def op(self) -> int:
        return self.ops[self.opsI][1]

    @property
    def op_range(self):
        return self.ops[self.opsI]

    def __repr__(self):
        if self.valid:
            return "{} Op:{}{} CigPos:{} RefPos:{} SeqPos:{} Base:{} Quality:{} RefBase:{}".format(self.record.name, self.op_length, "MIDNSHP=XB"[self.op], self.op_pos, self.ref_pos, self.seq_pos, self.seq_base, self.base_qual, self.ref_base)
        else:
            return "{} INVALID".format(self.record.name)