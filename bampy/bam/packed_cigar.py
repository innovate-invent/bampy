import ctypes as C

OP_CODES = tuple(b"MIDNSHP=X"[i:i+1] for i in range(9))

class PackedCIGAR:
    __slots__ = "buffer"

    def __init__(self, buffer: memoryview):
        self.buffer = buffer or bytearray()

    def __repr__(self):
        return "".join("{}{}".format(c[0], OP_CODES[c[1]].decode('ASCII')) for c in self)

    def __bytes__(self):
        return b"".join(str(c[0]).encode('ASCII') + OP_CODES[c[1]] for c in self)

    def __getitem__(self, i):
        if isinstance(i, slice):
            start = slice.start or 0
            stop = slice.stop or len(self)
            step = slice.step or 1
            return [self[a] for a in range(start, stop, step)]

        op = self.buffer[i]
        return op >> 4, op & 0b1111

    def __setitem__(self, i, value):
        if isinstance(i, slice):
            start = i.start or 0
            stop = i.stop or len(self)
            step = i.step or 1
            if hasattr(value, '__iter__') and len(value) == (stop - start) / step:
                for a, b in zip(range(start, stop, step), value):
                    self.buffer[a] = C.c_uint32.__ctype_le__(b)
            else:
                # TODO make cigar mutable, this can only work for new files
                raise ValueError("Slice assignment can not change length of sequence.")
        else:
            self.buffer[i] = value[0] << 4 | value[1]

    def __len__(self):
        return len(self.buffer)

    @staticmethod
    def pack(from_buffer, to_buffer=None):
        if isinstance(from_buffer, PackedCIGAR):
            # TODO memcpy to toBuffer
            return from_buffer
        packed = PackedCIGAR(to_buffer)
        packed[:] = from_buffer

        return packed

    def unpack(self):
        return list(self)

    def copy(self):
        return PackedCIGAR(bytearray(self.buffer))
