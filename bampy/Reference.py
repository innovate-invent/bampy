import ctypes as C


class Reference:
    __slots__ = 'name', 'length', 'index', '_optional'

    def __init__(self, name: str, length: int, index: int = 0, optional={}):
        self.name = name
        self.length = length
        self.index = index
        self._optional = optional

    def __repr__(self):
        rep = "@SQ SN:{} LN:{}".format(self.name, self.length)
        if len(self._optional):
            for k, v in self._optional.items():
                rep += " {}: {}".format(k, v)
        return rep

    def __bytes__(self):
        b = b'@SQ\tSN:' + self.name.encode('ASCII') + b'\tLN:' + str(self.length).encode('ASCII')
        if len(self._optional):
            for k, v in self._optional.items():
                b += b'\t' + k + b':' + v
        return b + b'\n'

    def pack(self):
        return ((len(self.name) + 1).to_bytes(C.sizeof(C.c_int32), 'little', signed=True)
                + self.name.encode('ascii') + b'\00'
                + self.length.to_bytes(C.sizeof(C.c_int32), 'little', signed=True))
