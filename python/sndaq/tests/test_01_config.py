
import os
import sndaq
from sndaq.analysis import AnalysisConfig

import unittest
import numpy.testing as npt


class TestConfig(unittest.TestCase):

    def test_from_config(self):
        """Analysis configuration from INI file
        """
        cfile = os.path.join(sndaq.base_path, 'data/config/analysis.config')
        config = AnalysisConfig.from_config(conf_path=cfile)

        # Test input settings.
        self.assertTrue(config.use_offsets)
        self.assertTrue(config.use_rebins)
        npt.assert_array_equal(config.binsize_ms, [500, 1500, 4000, 10000])
        self.assertEqual(config.duration_bgl_ms, 300000)
        self.assertEqual(config.duration_bgt_ms, 300000)
        self.assertEqual(config.duration_exl_ms, 15000)
        self.assertEqual(config.duration_ext_ms, 15000)
        self.assertEqual(config.min_active_doms, 100)
        self.assertEqual(config.min_bkg_rate, 1e2)
        self.assertEqual(config.max_bkg_rate, 1e4)
        self.assertEqual(config.min_bkg_fano, 0.8)
        self.assertEqual(config.max_bkg_fano, 0.2)
        self.assertEqual(config.max_bkg_abs_skew, 1.2)

        # Test __repr__.
        conf_str = """Analysis Configuration
======================
| 
| Buffer Configuration
| --------------------
| Use Offsets : True
| Use Rebins : True
| Trailing Background (ms): [ -315000, -15000 ]
| Trailing Exclusion (ms): [ -15000, 0 ]
| Search Windows (ms): [  500  1500  4000 10000]
| Leading Exclusion (ms): [ t_sw, t_sw + 15000 ]
| Leading Background (ms): [ t_sw + 15000, t_sw + 315000 ] 
|
| .. t_sw = search window upper bin edge
| 
| DOM Qualification
| -----------------
| Rate: [100.0, 10000.0]
| Fano Factor: [0.8, 0.2]
| Abs. Skew < 1.2
"""
        self.assertEqual(conf_str, config.__repr__())
