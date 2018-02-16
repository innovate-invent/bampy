"""
This subpackage contains all the code needed to work with BGZF compressed data.

Classes:
    Block: Represents a BGZF/GZIP block.
    Reader: Convenience interface to read in compressed data.
    Writer: Convenience interface to write compressed data.

Constants:
    EmptyBlock bytes: This is the byte data representing an empty block. This is used as an EOF marker at the end of BGZF compressed files.
    MAX_BLOCK_SIZE int: This is the maximum BGZF block size imposed by the domain of the two byte block size subfield value.

For more:
    >> help(bampy.bgzf.block) for more information on the Block object.
    >> help(bampy.bgzf.reader) for more information on the Reader object.
    >> help(bampy.bgzf.writer) for more information on the Writer object.
    >> help(bampy.bgzf.util) for more information on utility functions including functions to work with BGZF data.
    >> help(bampy.bgzf.zlib) for more information on the zlib wrapper.
"""

from .block import Block, MAX_CDATA_SIZE
from .reader import EmptyBlock, Reader
from .util import EMPTY_BLOCK, MAX_BLOCK_SIZE, SIZEOF_EMPTY_BLOCK, is_bgzf
from .writer import Writer
