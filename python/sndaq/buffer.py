"""Rolling buffers implemented in numpy
"""
import numpy as np
from abc import ABC, abstractmethod


class sndaqbuffer(ABC):
    """Base class for SNDAQ rolling data buffer
    """
    def __init__(self, size, ndom=5160, dtype=np.uint16):
        self._size = size
        self._ndom = ndom
        self._dtype = dtype
        super().__init__()

    @abstractmethod
    def append(self, entry):
        """Add entry to front of array

        Parameters
        ----------
        entry : numpy.ndarray
            ndom-length array of new values to add to array
        """
        pass

    @abstractmethod
    def clear(self):
        """Clear data buffer
        """
        pass

    @abstractmethod
    def __getitem__(self, key):
        pass

    @property
    @abstractmethod
    def data(self):
        """Retrieve data buffer
        """
        pass


class windowbuffer(sndaqbuffer):
    """Rolling buffer used for SNDAQ SICO analysis
    """
    def __init__(self, size, ndom=5160, dtype=np.uint16, mult=2):
        super().__init__(size, ndom, dtype)
        self._mult = mult
        self._buflen = self._size*self._mult
        self._n = 0
        self._data = np.zeros(shape=(self._buflen, self._ndom),
                              dtype=self._dtype)
        self._idx = self._size

    def append(self, entry):
        """Add entry to front of array

        Parameters
        ----------
        entry : numpy.ndarray
            ndom-length array of new values to add to array

        Returns
        -------
        self : windowbuffer
            A copy of current windowbuffer object

        """
        if self._idx >= self._buflen:
            self._reset()
        self._data[self._idx, :] = entry
        self._idx += 1
        if not self.filled:
            self._n += 1
        return self

    def clear(self):
        """Clear data buffer
        """
        self._data = np.zeros(shape=(self._buflen, self._ndom),
                              dtype=self._dtype)
        self._idx = self._size

    def _reset(self):
        self._idx = self._size
        self._data[:self._size, :] = self._data[-self._size:, :]

    def __getitem__(self, key):
        return self.data[key]

    @property
    def data(self):
        """Retrieve contents of rolling buffer

        Returns
        -------
        data : numpy.ndarray
            Contents of rolling buffer

        Notes
        -----
        The full array used is larger than the array returned by this method. The shifting idx variable allows, this
        data array to be treated as a rolling buffer. The full data array is provided by _data

        See Also
        --------
        _data

        """
        return self._data[self._idx-self._size:self._idx, :]

    @property
    def filled(self):
        """Indicates if buffer has been filled

        Returns
        -------
        has_filled : bool
            If True, buffer has appended a number of data columns equal to or greater than _buflen and has filled.
            If False, buffer has appended fewer than _buflen data columns
        """
        return self._n >= self._buflen

    @property
    def n(self):
        """Number of data columns appended

        Returns
        -------
        n : int
            Number of data columns that have been appended.

        Notes
        -----
        This will stop incrementing at a value equal to _buflen and thus will never exceed _buflen
        """
        return self._n


class stagingbuffer(sndaqbuffer):
    def __init__(self, size, ndom=5160, dtype=np.uint8, mult=2):
        super().__init__(size, ndom, dtype)
        self._mult = mult
        self._buflen = self._size * self._mult
        self.clear()

    def add(self, val, idx_row, idx_col):
        np.add.at(self._data, (idx_row, self._idx - self._size + idx_col), val)
        return self

    def append(self, entry):
        return self

    def advance(self):
        if self._idx >= self._buflen:
            self._reset()
        self._data[:, self._idx] = 0
        self._idx += 1
        return self

    def clear(self):
        self._data = np.zeros(shape=(self._ndom, self._buflen),
                              dtype=self._dtype)
        self._idx = self._size

    def _reset(self):
        self._idx = self._size-1
        # self._idx = 1
        self._data[:, :self._idx] = self._data[:, -self._idx:]

    def __getitem__(self, key):
        return self.data[key]

    @property
    def data(self):
        return self._data[:, self._idx - self._size:self._idx]

    @property
    def front(self):
        return self._data[:, self._idx - self._size]

