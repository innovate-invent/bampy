from . import bgzf, bam, sam
import io

def discoverStream(stream):
    peek = bytearray(4)
    stream.readinto(peek)
    if bgzf.is_bgzf(peek):
        return bgzf.Block.from_stream(stream, peek)
    elif bam.is_bam(peek):
        return bam.header_from_stream(stream, peek)
    else:
        #SAM
        return sam.header_from_stream(stream, peek)


def Reader(input, offset=0):
    if isinstance(input, (io.RawIOBase, io.BufferedIOBase)):
        # Stream
        peek = bytearray(4)
        input.readinto(peek)
        if bgzf.is_bgzf(peek):
            return BGZFReader(input, offset, peek)
        elif bam.is_bam(peek):
            return BAMStreamReader(input, peek)
        else:
            # SAM
            return SAMStreamReader(input, peek)
    else:
        if bgzf.is_bgzf(input, offset):
            return BGZFReader(input, offset)
        elif bam.is_bam(input, offset):
            return BAMBufferReader(input, offset)
        else:
            # SAM
            return SAMBufferReader(input, offset)

class _Reader:
    def __init__(self, input):
        self._input = input

    def __iter__(self):
        return self

    def __next__(self):
        raise NotImplementedError()

class StreamReader(_Reader):
    def __init__(self, input):
        super().__init__(input)

class BufferReader(_Reader):
    def __init__(self, input, offset=0):
        super().__init__(input)
        self.offset = offset

class BGZFReader(BufferReader):
    def __init__(self, input, offset=0, peek=None):
        self._bgzfReader = bgzf.Reader(input, offset, peek)
        while True:
            try:
                self.header, self.references, offset = bam.header_from_buffer(next(self._bgzfReader))
            except bam.util.BufferUnderflow:
                continue
            self._bgzfOffset = offset
            self._bgzfReader.remaining -= offset
            break

    def __next__(self):
        while True:
            try:
                record = bam.Record.from_buffer(self._bgzfReader.buffer, self._bgzfOffset, self.references)
                record_len = len(record)
                self._bgzfOffset += record_len
                self._bgzfReader.remaining -= record_len
                return record
            except bam.util.BufferUnderflow:
                next(self._bgzfReader)
                self._bgzfOffset = 0


class BAMStreamReader(StreamReader):
    def __init__(self, input, peek=None):
        self.header, self.references, _ = bam.header_from_stream(input, peek)

    def __next__(self):
        return bam.Record.from_stream(self._input, self.references)


class SAMStreamReader(StreamReader):
    def __init__(self, input, peek=None):
        self.header, self.references, _ = sam.header_from_stream(input, peek)

    def __next__(self):
        return bam.Record.from_sam(self._input.readline(), self.references)


class BAMBufferReader(BufferReader):
    def __init__(self, input, offset=0):
        self.header, self.references, self.offset = bam.header_from_buffer(input, offset)

    def __next__(self):
        record = bam.Record.from_buffer(self._input, self.offset, self.references)
        self.offset = len(record)
        return record


class SAMBufferReader(BufferReader):
    def __init__(self, input, offset=0):
        self.header, self.references, self.offset = sam.header_from_buffer(input, offset)

    def __next__(self):
        offset = self.offset
        end = self._input.find(b'\n', offset)
        self.offset = end + 1
        return bam.Record.from_sam(self._input[offset, end], self.references)