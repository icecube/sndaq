import sndaq
import unittest


class TestInit(unittest.TestCase):

    def test_version_exists(self):
        """Module initialization and version
        """
        self.assertTrue(hasattr(sndaq, '__version__'))
