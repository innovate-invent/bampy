import ctypes as C

SIZEOF_INT32 = C.sizeof(C.c_int32)


class Reference:
    """
    Represents a reference to which Records are aligned to.
    """
    __slots__ = 'name', 'length', 'index', '_optional'

    def __init__(self, name: str, length: int, index: int = 0, optional: dict = {}):
        """
        Constructor.
        :param name: Reference sequence name .
        :param length: Total length of the reference sequence.
        :param index: Index used to dereference record ids
        :param optional: Dict containing optional reference properties. See SQ header documentation.
        """
        self.name = name
        self.length = length
        self.index = index
        self._optional = optional

    def __repr__(self):
        """
        String representation of Reference data.
        See __bytes__() to convert to SAM.
        :return:
        """
        rep = "@SQ SN:{} LN:{}".format(self.name, self.length)
        if len(self._optional):
            for k, v in self._optional.items():
                rep += " {}: {}".format(k, v)
        return rep

    def __bytes__(self):
        """
        SAM representation of reference SQ header tag.
        :return: bytes object containing ASCII encoded data.
        """
        b = b'@SQ\tSN:' + self.name.encode('ASCII') + b'\tLN:' + str(self.length).encode('ASCII')
        if len(self._optional):
            for k, v in self._optional.items():
                b += b'\t' + k + b':' + v
        return b + b'\n'

    def pack(self):
        """
        Convert to BAM formatted bytes representation.
        :return: Bytes object instance containing data.
        """
        return ((len(self.name) + 1).to_bytes(SIZEOF_INT32, 'little', signed=True)
                + self.name.encode('ascii') + b'\00'
                + self.length.to_bytes(SIZEOF_INT32, 'little', signed=True))
