from unittest import TestCase

from bampy.bgzf import Writer, EMPTY_BLOCK, MAX_BLOCK_SIZE
from tests.bgzf.data import BLOCK_VALID


class TestBufferWriter(TestCase):

    def test_empty_data(self):
        buffer = bytearray(len(EMPTY_BLOCK))
        writer = Writer(buffer)
        writer(b'')
        del writer
        self.assertEqual(buffer, bytearray(len(EMPTY_BLOCK)), "Non-empty block written to buffer")

    def test_simple_data(self):
        buffer = bytearray(MAX_BLOCK_SIZE)
        writer = Writer(buffer)
        writer(bytearray(b'test123'))
        writer.finish_block()
        self.assertEqual(buffer[:writer.offset], BLOCK_VALID, "Invalid block written to buffer")
