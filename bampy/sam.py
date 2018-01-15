import re
import bampy.bam

#header_re = re.compile(r"^@[A-Za-z][A-Za-z](\t[A-Za-z][A-Za-z0-9]:[ -~]+)+$|^@CO\t.*")
cigar_re = re.compile(r"([0-9]+)([MIDNSHPX=])")

def header_from_stream(stream, _magic = None):
    pass

def header_from_buffer(buffer):
    pass

def streamReader(stream):
    pass

def bufferReader(buffer, offset):
    pass

def streamWriter(records, stream):
    pass

def bufferWriter(records, buffer, offset):
    pass