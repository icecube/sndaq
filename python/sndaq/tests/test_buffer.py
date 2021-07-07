import unittest
from sndaq.sndaqbuffer import windowbuffer


class TestWindowBuffer(unittest.TestCase):

    def test_buffersize(self):

        size = 20
        self.assertEquals(windowbuffer(size)._size, size)
