import numpy as np
from abc import ABC, abstractmethod


class sndaqbuffer(ABC):
    def __init__(self, size, ndom=5160, dtype=np.uint16):
        self._size = size
        self._ndom = ndom
        self._dtype = dtype
        super().__init__()

    @abstractmethod
    def append(self, entry):
        pass

    @abstractmethod
    def clear(self):
        pass

    @abstractmethod
    def __getitem__(self, key):
        pass

    @property
    @abstractmethod
    def data(self):
        pass


class windowbuffer(sndaqbuffer):
    def __init__(self, size, ndom=5160, dtype=np.uint16, mult=2):
        super().__init__(size, ndom, dtype)
        self._mult = mult
        self._buflen = self._size*self._mult
        self._n = 0
        self.clear()

    def append(self, entry):
        if self._idx >= self._buflen:
            self._reset()
        self._data[self._idx, :] = entry
        self._idx += 1
        if not self.filled:
            self._n += 1
        return self

    def clear(self):
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
        return self._data[self._idx-self._size:self._idx, :]

    @property
    def filled(self):
        return self._n >= self._buflen

    @property
    def nscaler(self):
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

