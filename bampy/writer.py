import io

from . import bam, bgzf, sam


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
        data = record.pack()
        record_len = len(record)
        if record_len < bgzf.MAX_CDATA_SIZE and self._output.block_remaining() < record_len:
            self._output.finish_block()
        it = iter(data)
        cur = next(it)
        try:
            while True:
                nxt = next(it)
                self._output(cur)
                cur = nxt
        except StopIteration:
            self._output(cur, boundary=True)

    @property
    def offset(self):
        if self._output:
            return self._output.offset
        else:
            return self._offset

    def finalize(self):
        if self._output:
            self._output.finish_block()
            offset = self._output.offset
            output = self._output._output
            if isinstance(output, (io.RawIOBase, io.BufferedIOBase)):
                output.write(bgzf.EMPTY_BLOCK)
            else:
                output[offset:offset + bgzf.SIZEOF_EMPTY_BLOCK] = bgzf.EMPTY_BLOCK
            self._offset = offset + bgzf.SIZEOF_EMPTY_BLOCK
            self._output = None

    def __del__(self):
        self.finalize()
