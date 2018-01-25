from .record import Record
from .tag import Tag
from .packed_sequence import SEQUENCE_VALUES
from .packed_cigar import OP_CODES
from .util import header_to_buffer, header_to_stream, header_from_buffer, header_from_stream, pack_header, is_bam