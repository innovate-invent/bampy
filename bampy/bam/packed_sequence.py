from . import SEQUENCE_VALUES


class PackedSequence:
    __slots__ = "buffer", "_length"

    def __init__(self, buffer: memoryview or None, length):
        if buffer:
            self.buffer = buffer
        else:
            self.buffer = bytearray((length + 1) // 2)

        self._length = length

    def __repr__(self):
        return "".join(SEQUENCE_VALUES[c] for c in self)

    def __bytes__(self):
        return b"".join(SEQUENCE_VALUES[c] for c in self)

    def __getitem__(self, i):
        if isinstance(i, slice):
            start = i.start or 0
            stop = i.stop or len(self)
            step = i.step or 1
            s = bytearray((stop - start) // step)
            for a in range(start, stop, step):
                s[a] = self[a]
            return s

        if i % 2:
            return self.buffer[i // 2] & 0b00001111
        else:
            return self.buffer[i // 2] >> 4

    def __setitem__(self, i, value):
        if isinstance(i, slice):
            start = i.start or 0
            stop = i.stop or len(self)
            step = i.step or 1
            if hasattr(value, '__iter__') and len(value) == (stop - start) // step:
                # TODO optimise pairwise packing if step == 1
                for a, b in zip(range(start, stop, step), value):
                    if a % 2:
                        self.buffer[a // 2] = (self.buffer[a // 2] & 0b11110000) | b
                    else:
                        self.buffer[a // 2] = (self.buffer[a // 2] & 0b00001111) | (b << 4)
            else:
                # TODO make sequence mutable, this can only work for new files
                raise ValueError("Slice assignment can not change length of sequence.")
        else:
            if i % 2:
                self.buffer[i // 2] = (self.buffer[i // 2] & 0b11110000) | value
            else:
                self.buffer[i // 2] = (self.buffer[i // 2] & 0b00001111) | (value << 4)

    def __iter__(self):
        count = 1
        for b in self.buffer:
            yield b >> 4
            if count < self._length:
                yield b & 0b00001111
            count += 2

    def __reversed__(self):
        odd = self._length & 1
        for b in reversed(self.buffer):
            if odd:
                odd = False
            else:
                yield b & 0b00001111
            yield b >> 4

    def __len__(self):
        return self._length

    @staticmethod
    def pack(from_buffer, to_buffer=None):
        if isinstance(from_buffer, PackedSequence):
            # TODO memcpy to to_buffer
            return from_buffer
        packed = PackedSequence(to_buffer, len(from_buffer))
        packed[:] = from_buffer

        return packed

    def unpack(self):
        return bytes(iter(self))

    def copy(self):
        return PackedSequence(bytearray(self.buffer), self._length)