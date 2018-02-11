from unittest import TestCase

from bampy.bgzf import Reader, EMPTY_BLOCK

from .data import BLOCK_VALID

class TestBufferReader(TestCase):

    def test_empty_block(self):
        with self.assertRaises(StopIteration):
            reader = Reader(bytearray(EMPTY_BLOCK))
            data = next(reader)

    def test_valid_block(self):
        with self.assertRaises(StopIteration):
            reader = Reader(bytearray(BLOCK_VALID))
            data = next(reader)
            self.assertEqual(bytes(data), b'test123', "VALID: Incorrect data")
            data = next(reader)
            self.assertEqual(bytes(data), b'', "VALID: Extra data found")

