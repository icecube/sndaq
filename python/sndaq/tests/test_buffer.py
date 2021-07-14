import unittest
import numpy as np
from sndaq.buffer import sndaqbuffer, windowbuffer


class testSndaqBuffer(unittest.TestCase):

    def test_init(self):
        with self.assertRaises(TypeError):
            sndaqbuffer(1)


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
        self.assertEqual(buffer._size, size)
        self.assertEqual(buffer.data.size, size*ndom)

    def test_get(self):
        # All zeros are expected
        self.assertFalse(np.any(windowbuffer(20)[0]), "Found non-zero value")

    def test_append(self):
        size = 20
        ndom = 40
        buffer = windowbuffer(size=size, ndom=ndom)
        values = np.asarray(np.random.randint(0, 255, size=ndom), dtype=np.uint16)
        buffer.append(values)
        self.assertTrue(np.all(buffer[-1] == values))

    def test_reset(self):
        size = 4
        ndom = 10
        n = 1
        buffer = windowbuffer(size=size, ndom=ndom, mult=2)
        self.assertEqual(buffer._idx, size)
        for i in range(size):
            buffer.append(n*np.ones(ndom, dtype=np.uint16))
            n += 1

        value_before_reset = np.array(buffer.data)
        self.assertEqual(buffer._idx, buffer._mult*size)
        buffer.append(n*np.ones(ndom, dtype=np.uint16))
        value_after_reset = np.vstack((value_before_reset[1:],n*np.ones(ndom, dtype=np.uint16)))
        n += 1

        self.assertTrue(np.all(buffer._data[:size] == value_before_reset))
        self.assertTrue(np.all(buffer.data == value_after_reset))
        self.assertEqual(buffer._idx, size+1)

        for i in range(size-1):
            buffer.append(n*np.ones(ndom, dtype=np.uint16))
            n += 1
        self.assertEqual(buffer._idx, size*2)

