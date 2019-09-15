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
		...

    @abstractmethod
    def clear(self):
        ...

    def __getitem__(self, key):
        return self.data[key]

    @property
    @abstractmethod
    def data(self):
        ...


class windowbuffer(sndaqbuffer):
    def __init__(self, size, ndom=5160, dtype=np.uint16, mult=2):
        super().__init__(size, ndom, dtype)
        self._mult = mult
        self._buflen = self._size*self._mult
        self.clear()

    def append(self, entry):
        if self._idx >= self._buflen:
            self._reset()
        self._data[self._idx, :] = entry
        self._idx += 1
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
