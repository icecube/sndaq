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

    def __init__(self, doms_table='../../data/config/full_dom_table.txt'):
        # May need to update file, and perform filtering on IceTop DOMs (08/09/21 SG)
        self.dom_table = np.genfromtxt(doms_table, dtype=dom_table_dtypes,
                                       converters={5: lambda s: int(s, 16)}, encoding=None)
        self._dom_id_map = np.argsort(self.dom_table['mbid'])
        self._dom_id_sorted = self.dom_table['mbid'][self._dom_id_map]

    def get_dom_idx(self, dom_id):
        idx = self._dom_id_map[self._dom_id_sorted == dom_id]
        if idx.size == 0:
            raise RuntimeError('Unknown DOM ID, no match found during lookup')
        elif idx.size > 1:
            raise RuntimeError('Multiple matching DOM IDs found during lookup: {0}'.format(idx))
        return idx[0]
