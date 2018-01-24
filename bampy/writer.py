from . import sam, bam, bgzf

class Writer:
    def __init__(self):
        raise NotImplementedError("Must use .sam(), .bam(), or .bgzf() constructors.")

    @staticmethod
    def sam(output, offset=0, sam_header=b'', references=()):
        new = Writer.__new__(Writer)
        new._output = output
        new.references = references
        sam_header = sam.pack_header(sam_header, references)
        if isinstance(input, 'RawIOBase'):
            new.__call__ = Writer._sam_stream
            output.write(sam_header)
        else:
            new.__call__ = Writer._sam_buffer
            sam_len = len(sam_header)
            output[offset : offset + sam_len] = sam_header
            new.offset = offset + sam_len
        return new

    @staticmethod
    def bam(output, offset=0, sam_header=b'', references=()):
        new = Writer.__new__(Writer)
        new._output = output
        new.references = references
        if isinstance(sam_header, dict):
            sam_header = sam.pack_header(sam_header, references)
        if isinstance(input, 'RawIOBase'):
            new.__call__ = Writer._bam_stream
            bam.header_to_stream(output, sam_header, references)
        else:
            new.__call__ = Writer._bam_buffer
            new.offset = bam.header_to_buffer(output, offset, sam_header, references)
        return new

    @staticmethod
    def bgzf(output, offset=0, sam_header=b'', references=()):
        new = Writer.__new__(Writer)
        new._output = bgzf.Writer(output, offset)
        new.__call__ = Writer._bgzf
        new.references = references
        new._output(bam.pack_header(sam_header, references))
        return new

    def _sam_stream(self, record):
        self._output.writeline(bytes(record))

    def _sam_buffer(self, record):
        r = bytes(record)
        l = len(r)
        self._output[self.offset:self.offset + l] = r
        self.offset += l

    def _bam_stream(self, record):
        record.to_stream(self._output)

    def _bam_buffer(self, record):
        record.to_buffer(self._output, self.offset)
        self.offset += len(record)

    def _bgzf(self, record):
        record.pack()
        if self._output.block_remaining() < len(record):
            self._output.finish_block()
        self._output(record._header)
        self._output(record.name)
        self._output(record.cigar)
        self._output(record.sequence)
        self._output(record.quality_scores)
        for tag in record.tags.values():
            self._output(tag._header)
            self._output(tag._buffer)

    def __call__(self, *args, **kwargs):
        raise NotImplementedError()