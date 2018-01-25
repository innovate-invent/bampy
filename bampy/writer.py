from . import sam, bam, bgzf
import io

class Writer:
    def __init__(self, output, references=()):
        self._output = output
        self.references = references

    @staticmethod
    def sam(output, offset=0, sam_header=b'', references=()):
        sam_header = sam.pack_header(sam_header, references)
        if isinstance(output, (io.RawIOBase, io.BufferedIOBase)):
            output.write(sam_header)
            return SAMStreamWriter(output, references)
        else:
            sam_len = len(sam_header)
            output[offset: offset + sam_len] = sam_header
            return SAMBufferWriter(output, offset + sam_len, references)

    @staticmethod
    def bam(output, offset=0, sam_header=b'', references=()):
        sam_header = sam.pack_header(sam_header, references)
        if isinstance(output, (io.RawIOBase, io.BufferedIOBase)):
            bam.header_to_stream(output, sam_header, references)
            return BAMStreamWriter(output, references)
        else:
            return BAMBufferWriter(output, bam.header_to_buffer(output, offset, sam_header, references), references)

    @staticmethod
    def bgzf(output, offset=0, sam_header=b'', references=()):
        writer = BGZFWriter(output, offset, references)
        writer._output(bam.pack_header(sam_header, references))
        writer._output.finish_block()
        return writer

    def __call__(self, *args, **kwargs):
        raise NotImplementedError()


class StreamWriter(Writer):
    def __init__(self, output, references):
        super().__init__(output, references)


class BufferWriter(Writer):
    def __init__(self, output, offset=0, references=()):
        super().__init__(output, references)
        self.offset = offset


class SAMStreamWriter(Writer):
    def __call__(self, record):
        self._output.writelines((bytes(record), b'\n'))


class SAMBufferWriter(BufferWriter):
    def __call__(self, record):
        r = bytes(record) + b'\n'
        l = len(r)
        self._output[self.offset:self.offset + l] = r
        self.offset += l


class BAMStreamWriter(StreamWriter):
    def __call__(self, record):
        record.to_stream(self._output)


class BAMBufferWriter(BufferWriter):
    def __call__(self, record):
        record.to_buffer(self._output, self.offset)
        self.offset += len(record)


class BGZFWriter(Writer):
    def __init__(self, output, offset=0, references=()):
        super().__init__(bgzf.Writer(output, offset), references)

    def __call__(self, record):
        record.pack()
        record_len = len(record)
        if record_len < bgzf.MAX_CDATA_SIZE and self._output.block_remaining() < record_len:
            self._output.finish_block()
        self._output(record._header)
        self._output(record.name)
        self._output(record.cigar.buffer)
        self._output(record.sequence.buffer)
        self._output(record.quality_scores)
        for tag in record.tags.values():
            self._output(tag.pack())