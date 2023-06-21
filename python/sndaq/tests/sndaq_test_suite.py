import unittest

def sndaq_test_suite():
    """Returns unittest.TestSuite of sndaq tests.
    """
    from os.path import dirname
    pydir = dirname(dirname(__file__))
    tests = unittest.defaultTestLoader.discover(pydir,
                                                top_level_dir=dirname(pydir))
    return tests

def runtests():
    """Run all tests in sndaq.tests.test_*.py
    """
    # Load all TestCase classes from sndaq/tests/test_*.py
    tests = sndaq_test_suite()
    unittest.TextTestRunner(verbosity=2).run(tests)

if __name__ == '__main__':
    runtests()
