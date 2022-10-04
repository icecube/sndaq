"""IceCube Detector, DOM Tables, and related utility functions
"""
import numpy as np

dom_table_dtypes = [
    ('str', 'f4'),
    ('i', 'f4'),
    ('x', 'f8'),
    ('y', 'f8'),
    ('z', 'f8'),
    ('mbid', 'u8'),
    ('type', 'U2'),
    ('effvol', 'f8')]


class Detector:
    """IceCube Detector. An object containing DOM tables, maps for DOM IDs and DOM validation functions
    """
    def __init__(self, doms_table='../../data/config/full_dom_table.txt'):
        # May need to update file, and perform filtering on IceTop DOMs (08/09/21 SG)
        self.dom_table = np.genfromtxt(doms_table, dtype=dom_table_dtypes,
                                       converters={5: lambda s: int(s, 16)}, encoding=None)
        self._dom_id_map = np.argsort(self.dom_table['mbid'])
        self._dom_id_sorted = self.dom_table['mbid'][self._dom_id_map]

    def get_dom_idx(self, dom_id):
        """Get index corresponding to DOM ID

        Parameters
        ----------
        dom_id : int
            DOM motherboard ID in base 10 integer format

        Returns
        -------
        idx : int
            Index of DOM Table corresponding to matching DOM ID in _dom_id_sorted

        Notes
        -----
        The sorting of _dom_id_sorted is assumed to be the sorting of all data arrays used for SICO analyses

        """
        idx = self._dom_id_map[self._dom_id_sorted == dom_id]
        if idx.size == 0:
            raise RuntimeError('Unknown DOM ID, no match found during lookup')
        elif idx.size > 1:
            raise RuntimeError('Multiple matching DOM IDs found during lookup: {0}'.format(idx))
        return idx[0]

    def isvalid_dom(self, dom_id):
        """Determine if DOM is included in SNDAQ SICO analysis

        Parameters
        ----------
        dom_id : int
            DOM motherboard ID in base 10 integer format

        Returns
        -------
        isvalid : bool
            If True, DOM with given ID contributes to SNDAQ SICO analysis
            If False, DOM with given ID does not contribute
        """
        return np.any(self._dom_id_sorted == dom_id)
