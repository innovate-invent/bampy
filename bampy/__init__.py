from .__version import __version__

import bampy.bam, bampy.bgzf
from bampy.bam import SEQUENCE_VALUES, OP_CODES, Record

# if read only access_copy
# if write access_write
#path = os.path.expanduser(path)
#fh = open(path, mode + 'b')
#return mmap.mmap(fh.fileno(), 1000 if 'w' in mode else 0, access=mmap.ACCESS_WRITE if fh.writeable() else mmap.ACCESS_COPY)


