import ctypes as C

class Reference:
    __slots__ = 'name', 'length'
    def __init__(self, name: str, length: int):
        self.name = name #type: str
        self.length = length #type: int
        #TODO allow additional SAM attributes

    def __repr__(self):
        return "@SQ SN:{} LN:{}".format(self.name, self.length)

    def __bytes__(self):
        return b'@SQ\tSN:' + self.name.encode('ASCII') + b'\tLN:' + str(self.length).encode('ASCII') + b'\n'

    def pack(self):
        return ((len(self.name) + 1).to_bytes(C.sizeof(C.c_int32), 'little', signed=True)
               + self.name.encode('ascii') + b'\00'
               + self.length.to_bytes(C.sizeof(C.c_int32), 'little', signed=True))