import unittest
import numpy as np
from sndaq.buffer import windowbuffer


class TestWindowBuffer(unittest.TestCase):

    def test_buffersize(self):
        size = 20
        self.assertEquals(windowbuffer(size)._size, size)

    def test_get(self):
        expected = np.zeros(5160, dtype=np.uint16)
        self.assertTrue(np.all(windowbuffer(20)[0] == expected))
