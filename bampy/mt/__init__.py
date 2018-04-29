from .reader import Reader
from .writer import Writer

from .bam import Record, OP_CODES, SEQUENCE_VALUES, CONSUMES_QUERY, CONSUMES_REFERENCE
from ..reader import discover_stream
from ..reference import Reference
