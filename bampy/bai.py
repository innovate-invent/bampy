import ctypes as C

MAGIC = b'BAI\1'


class Chunk(C.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ("begin", C.c_uint64),  # (Virtual) file offset of the start of the chunk
        ("end", C.c_uint64),  # (Virtual) file offset of the end of the chunk
    ]


class PseudoChunk(Chunk):
    _pack_ = 1
    _fields_ = [
        ("mapped", C.c_uint64),  # Number of mapped read-segments for this reference
        ("unmapped", C.c_uint64),  # Number of unmapped read-segments for this reference
    ]


SIZEOF_CHUNK = C.sizeof(Chunk)
SIZEOF_PSEUDOCHUNK = C.sizeof(PseudoChunk)
SIZEOF_UINT64 = C.sizeof(C.c_uint64)


def read(stream) -> (list, list, int):
    """
    Read in BAI index data.
    Returned tuple is composed of the following:
    [0] List of dicts indexed by reference id. Dict elements are keyed on bin number and values are arrays of Chunks.
    [1] List of arrays of virtual file offsets for each 16kbp reference interval. Indexed by reference id.
    [2] Number of unplaced unmapped reads (RNAME *), None if not present in BAI.
    :param stream: Readable stream containing BAI formatted data.
    :return: 3 element tuple (list, list, int)
    """
    assert stream.read(4) == MAGIC, "Unknown or corrupt input data."
    n_ref = int.from_bytes(stream.read(4), byteorder='little', signed=True)  # INT32
    bins = [None] * n_ref
    intervals = [None] * n_ref
    n_no_coor = None
    try:
        for ref in range(n_ref):
            # Read in bins
            bins[ref] = {}
            n_bin = int.from_bytes(stream.read(4), byteorder='little', signed=True)  # INT32
            while n_bin > 0:
                bin = int.from_bytes(stream.read(4), byteorder='little', signed=False)  # UINT32
                n_chunk = int.from_bytes(stream.read(4), byteorder='little', signed=True)  # INT32
                if bin == 37450:  # Detect pseudo-chunks
                    bins[ref][bin] = PseudoChunk.from_buffer(stream.read(SIZEOF_CHUNK * n_chunk))
                    n_bin -= 2
                else:
                    bins[ref][bin] = (Chunk * n_chunk).from_buffer(stream.read(SIZEOF_CHUNK * n_chunk))
                    n_bin -= 1

            # Read in intervals
            n_intv = int.from_bytes(stream.read(4), byteorder='little', signed=True)  # INT32
            intervals[ref] = (C.c_uint64 * n_intv).from_buffer(stream.read(SIZEOF_UINT64 * n_intv))

        n_no_coor = int.from_bytes(stream.read(8), byteorder='little', signed=False)  # UINT64
    except EOFError:
        pass
    return bins, intervals, n_no_coor


def write(stream, bins: list, intervals: list, unaligned: int = None) -> None:
    """
    Write out bins, list, and unaligned in BAI format
    :param stream: Writable output stream
    :param bins: List of dicts indexed by reference id. Dict elements are keyed on bin number and values are arrays of Chunks.
    :param intervals: List of arrays of virtual file offsets for each 16kbp reference interval. Indexed by reference id.
    :param unaligned: Number of unplaced unmapped reads (RNAME *) or None to omit from output.
    :return: None
    """
    stream.write(MAGIC)
    n_ref = len(bins)
    assert n_ref == len(intervals), "Reference count mismatch between bins and intervals."
    # n_ref
    stream.write(n_ref.to_bytes(4, 'little', True))
    for ref in range(n_ref):
        # Write bins
        # n_bin
        stream.write(len(bins[ref]).to_bytes(4, 'little', True))
        for bin, chunks in bins[ref].items():  # type: (int, Chunk)
            # bin
            stream.write(bin.to_bytes(4, 'little', False))
            # n_chunk
            stream.write(
                sum(2 if isinstance(chunk, PseudoChunk) else 1 for chunk in chunks).to_bytes(4, 'little', True)
            )
            for chunk in chunks:
                stream.write(chunk)

        # Write intervals
        stream.write(intervals[ref])

    if unaligned:
        stream.write(unaligned.to_bytes(8, 'little', False))
