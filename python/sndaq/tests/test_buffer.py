import unittest
import numpy as np
from sndaq.buffer import windowbuffer


class TestWindowBuffer(unittest.TestCase):

    def test_init(self):
        buffer = windowbuffer(1)
        self.assertEqual(buffer._ndom, 5160)
        self.assertEqual(buffer._mult, 2)
        self.assertEqual(buffer._idx, 1)
        self.assertIs(buffer._dtype, np.uint16)
        self.assertFalse(np.any(buffer.data))

    def test_size(self):
        size = 20
        ndom = 40
        buffer = windowbuffer(size=size, ndom=ndom)
        self.assertEquals(buffer._size, size)
        self.assertEquals(buffer.data.size, size*ndom)

    def test_get(self):
        # All zeros are expected
        self.assertFalse(np.any(windowbuffer(20)[0]), "Found non-zero value")


