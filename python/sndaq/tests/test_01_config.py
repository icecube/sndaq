
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

