import ctypes as C

MAGIC = b'\x1F\x8B'
"""bytes: Magic bytes identifying BGZF block"""

MAX_BLOCK_SIZE = 2 ** 16 - 1
"""int: This is the maximum BGZF block size imposed by the domain of the two byte block size subfield value."""

MAX_BLOCK_BUFFER_TYPE = C.c_ubyte * MAX_BLOCK_SIZE
"""C.Array: The ctypes array type instance representing the maximum block allocation."""

EMPTY_BLOCK = b'\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00\x42\x43\x02\x00\x1b\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00'
"""bytes: This is the byte data representing an empty block. This is used as an EOF marker at the end of BGZF compressed files."""

SIZEOF_EMPTY_BLOCK = len(EMPTY_BLOCK)
"""int: Number of bytes that the empty block occupies."""


def is_bgzf(buffer, offset=0):
    """
    Helper to determine if passed buffer contains a BGZF block.
    :param buffer: Buffer containing unknown data.
    :param offset: Offset into buffer to being reading.
    :return: True if offset points to beginning of a BGZF block, False otherwise.
    """
    return buffer[offset:offset + 2] == MAGIC


class InvalidBGZF(ValueError):
    """
    Exception to indicate invalid or unexpected data was read while trying to parse BGZF data.
    """
    pass
