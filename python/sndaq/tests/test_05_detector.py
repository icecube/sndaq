import unittest
from sndaq.detector import Detector

class TestDetector(unittest.TestCase):

    def test_init(self):
        """Detector initialization
        """
        detector = Detector(doms_table='./etc/full_dom_table.txt')
        self.assertEqual(detector._dom_id_map.size, 5160)
        self.assertEqual(detector._dom_id_map[5000], 585)
        self.assertEqual(detector._dom_id_sorted[5000], 272593794653359)

    def test_get_dom_idx(self):
        """Corresponding index
        """
        detector = Detector(doms_table='./etc/full_dom_table.txt')
        self.assertEqual(detector.get_dom_idx(272593794653359), 585)

    def test_isvalid_dom(self):
        """DOM in SNDAQ SICO analysis
        """
        detector = Detector(doms_table='./etc/full_dom_table.txt')
        self.assertFalse(detector.isvalid_dom(1))
        self.assertTrue(detector.isvalid_dom(272593794653359))
