from . import bgzf, bam, sam

def discoverStream(stream):
    peek = bytearray(4)
    stream.readinto(peek)
    if bgzf.isBGZF(peek):
        return bgzf.Block.from_stream(stream, peek)
    elif bam.isBAM(peek):
        return bam.header_from_stream(stream, peek)
    else:
        #SAM
        return sam.header_from_stream(stream, peek)

class Reader:
    def __init__(self, input, offset:int = 0):
        self._input = input
        if isinstance(input, 'RawIOBase'):
            # Stream
            peek = bytearray(4)
            input.readinto(peek)
            if bgzf.isBGZF(peek):
                self._bgzfReader = bgzf.Reader(input, peek=peek)
                while True:
                    try:
                        self.header, self.references, offset = bam.header_from_buffer(next(self._bgzfReader))
                    except bam.BufferUnderflow:
                        continue
                    self._bgzfOffset = offset
                    self._bgzfReader.remaining -= offset
                    break
                self.__next__ = self._BGZF
            elif bam.isBAM(peek):
                self.header, self.references, _ = bam.header_from_stream(input, peek)
                self.__next__ = self._BAM_stream
            else:
                # SAM
                self.header, self.references, _ = sam.header_from_stream(input, peek)
                self.__next__ = self._SAM_stream
        else:
            if bgzf.isBGZF(input, offset):
                self._bgzfReader = bgzf.Reader(input, offset)
                while True:
                    try:
                        self.header, self.references, offset = bam.header_from_buffer(next(self._bgzfReader))
                    except bam.BufferUnderflow:
                        continue
                    self._bgzfOffset = offset
                    self._bgzfReader.remaining -= offset
                    break
                self.offset = self._bgzfReader.offset
                self.__next__ = self._BGZF
            elif bam.isBAM(input, offset):
                self.header, self.references, self.offset = bam.header_from_buffer(input, offset)
                self.__next__ = self._BAM_buffer
            else:
                # SAM
                self.header, self.references, self.offset = sam.header_from_buffer(input, offset)
                self.__next__ = self._SAM_buffer

    def _BGZF(self):
        while True:
            try:
                record = bam.Record.from_buffer(self._bgzfReader.buffer, self._bgzfOffset, self.references)
                record_len = len(record)
                self._bgzfOffset += record_len
                self._bgzfReader.remaining -= record_len
                return record
            except bam.BufferUnderflow:
                next(self._bgzfReader)
                self._bgzfOffset = 0

    def _BAM_stream(self):
        return bam.Record.from_stream(self._input, self.references)

    def _SAM_stream(self):
        return bam.Record.from_sam(self._input.readline(), self.references)

    def _BAM_buffer(self):
        record = bam.Record.from_buffer(self._input, self.offset, self.references)
        self.offset = len(record)
        return record

    def _SAM_buffer(self):
        offset = self.offset
        end = self._input.find(b'\n', offset)
        self.offset = end + 1
        return bam.Record.from_sam(self._input[offset, end], self.references)

    def __iter__(self):
        return self

    def __next__(self):
        raise NotImplementedError()