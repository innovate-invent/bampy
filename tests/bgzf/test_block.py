from unittest import TestCase
import io

from bampy.bgzf import Block, EMPTY_BLOCK

from .data import BLOCK_VALID


class TestBlock(TestCase):
    def test_from_buffer(self):
        # Empty Block
        block, cdata = Block.from_buffer(bytearray(EMPTY_BLOCK))
        self.assertEqual(len(cdata), 2, "EMPTY: CDATA expected to be length 2")
        self.assertEqual(block.size, 28, "EMPTY: Incorrect block size")

        # Valid block w. data
        block, cdata = Block.from_buffer(bytearray(BLOCK_VALID))
        self.assertEqual(len(cdata), 9, "VALID: CDATA expected to be length 9")
        self.assertEqual(block.size, 35, "VALID: Incorrect block size")

        # Invalid magic identifier


        # Missing BC


        # Extra subfields

    def test_from_stream(self):
        # Empty Block
        block, cdata = Block.from_stream(io.BytesIO(EMPTY_BLOCK))
        self.assertEqual(len(cdata), 2, "EMPTY: CDATA expected to be length 2")
        self.assertEqual(block.size, 28, "EMPTY: Incorrect block size")

        # Valid block w. data
        block, cdata = Block.from_stream(io.BytesIO(BLOCK_VALID))
        self.assertEqual(len(cdata), 9, "VALID: CDATA expected to be length 9")
        self.assertEqual(block.size, 35, "VALID: Incorrect block size")

        # Invalid magic identifier

        # Missing BC

        # Extra subfields

   #def test__parseExtra(self):
   #    self.fail()

   #def test__getSize(self):
   #    self.fail()
