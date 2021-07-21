import numpy as np
from sndaq.buffer import windowbuffer


class AnalysisConfig:

    # IMPORTANT NOTE: Leading/trailing refers to time, buffer indices will be inversely prop to time
    # TODO: Documentation (Figure) or fix to Buffer indexing needed
    _raw_binsize = 2  # ms
    _base_binsize = 500  # ms
    _dur_leading_bg = 300  # ms
    _dur_trailing_bg = 300  # ms
    _dur_leading_excl = 15  # ms
    _dur_trailing_excl = 15e3  # ms

    def __init__(self, raw_binsize=None, base_binsize=None, dur_bgl=None, dur_bgt=None, dur_exl=None, dur_ext=None):
        # TODO: Define these using ConfigParser
        if raw_binsize is not None:
            AnalysisConfig._raw_binsize = raw_binsize
        if base_binsize is not None:
            AnalysisConfig._base_binsize = base_binsize
        if dur_bgl is not None:
            AnalysisConfig._dur_leading_bg = dur_bgl  # ms
        if dur_bgt is not None:
            AnalysisConfig._dur_trailing_bg = dur_bgt  # ms
        if dur_exl is not None:
            AnalysisConfig._dur_leading_excl = dur_exl  # ms
        if dur_ext is not None:
            AnalysisConfig._dur_trailing_excl = dur_ext  # ms

    @property
    def duration_nosearch(self):
        """
        :return: Duration of search window including background and exclusion blocks in ms
        :rtype: int
        """
        return self.dur_leading_bg + self.dur_leading_excl + self.dur_trailing_bg + self.dur_trailing_excl

    @property
    def raw_binsize(self):
        return self._raw_binsize

    @property
    def base_binsize(self):
        return self._base_binsize

    @property
    def dur_leading_bg(self):
        return self._dur_leading_bg

    @property
    def dur_trailing_bg(self):
        return self._dur_trailing_bg

    @property
    def dur_leading_excl(self):
        return self._dur_leading_excl

    @property
    def dur_trailing_excl(self):
        return self._dur_trailing_excl


class AnalysisHandler(AnalysisConfig):

    def __init__(self, binnings=(500, 1.5e3, 4e3, 10e3), ndom=5160, dtype=np.uint16):
        super().__init__()

        # Create shared window buffer
        self._binnings = binnings
        self._ndom = ndom
        self._dtype = dtype

        # minimum size for bg, excl, search, and largest search offset
        size = ((self.duration_nosearch + 2*int(max(binnings))) / self.base_binsize) - 1
        self.buffer_analysis = windowbuffer(size=size, ndom=ndom, dtype=dtype)
        self._rebin_factor = int(self.base_binsize/self.raw_binsize)
        self.buffer_raw = windowbuffer(size=size*self._rebin_factor, ndom=ndom, dtype=dtype)

        # Create analyses
        self.analyses = []
        for binning in np.asarray(binnings, dtype=dtype):
            for offset in np.arange(0, binning, 500, dtype=dtype):
                idx = size - (self.duration_nosearch + offset + binning)/self.base_binsize
                self.analyses.append(
                    Analysis(binning, offset, idx=idx, ndom=ndom)
                )

        # Define counter for accumulation used in rebinning from raw to base analysis
        self._accum_count = self._rebin_factor
        self._accum_data = np.zeros(ndom, dtype=dtype)

    def print_analyses(self):
        for i, analysis in enumerate(self.analyses):
            print(f'{i:d} {analysis.binsize*1e-3:4.1f} (+{analysis.offset*1e-3:4.1f})')

    def update_analyses(self, value):
        for analysis in self.analyses:
            self.update_sums(analysis, value)

    def update_sums(self, analysis, value):
        # Analysis sums are updated by the handler b/c the handler has access to buffers whereas analysis objects do not
        # TODO: May want to add check eventually if asymmetric bg/excl window is used
        analysis.hit_sum += self.buffer_analysis[analysis.idx_exl] + value
        analysis.hit_sum -= self.buffer_analysis[analysis.idx_bgl] + self.buffer_analysis[analysis.idx_bgt]

        analysis.hit_sum2 += self.buffer_analysis[analysis.idx_exl]**2 + value**2
        analysis.hit_sum2 -= self.buffer_analysis[analysis.idx_bgl]**2 + self.buffer_analysis[analysis.idx_bgt]**2

        analysis.rate += self.buffer_analysis[analysis.idx_ext]
        analysis.rate -= self.buffer_analysis[analysis.idx_sw]

    def accumulate(self, val):
        # This could be it's own class/component, maybe use itertools? Maybe use a generator w/ yield?
        self._accum_data += val
        self._accum_count -= 1
        return bool(self._accum_count)

    def reset_accumulator(self):
        self._accum_data = np.zeros(self._ndom, dtype=self._dtype)
        # _ndom and _dtype could be removed if this was part of an accumulator object, defined during init.
        self._accum_count = self._rebin_factor

    def update(self, value):
        """ Update raw buffer, analysis buffer, and analyses sums
        :param value: 2ms data for each DOM at a particular timestamp
        :type value: np.ndarray
        :return: None
        :rtype: None
        """
        self.buffer_raw.append(value)
        if not self.accumulate(value):  # Accumulator indicates time to reset, as base analysis bin of data is ready
            # There's almost certainly a better way to do this.
            accumulated_data = np.asarray(self._accum_data, dtype=np.uint16)
            self.reset_accumulator()
            self.update_analyses(accumulated_data)
            self.buffer_analysis.append(accumulated_data)

    def check_for_trigger(self, threshold=8.4, corr_threshold=5.8):
        raise NotImplementedError


class Analysis(AnalysisConfig):

    def __init__(self, binsize, offset, idx=0, ndom=5160, eps=np.ones(5160), dtype=np.uint16):
        super().__init__()
        if self.base_binsize % binsize:
            raise RuntimeError(f'Binsize {binsize:d} ms is incompatible, must be factor of {self.base_binsize:d} ms')
        self._binsize = binsize  # ms
        self._offset = offset  # ms
        self._rebinfactor = self._binsize / self.base_binsize
        self._ndom = ndom

        # Indices for accessing data buffer, all point to first column in respective region
        self._idx_bgl = idx  # Leading background window
        self._idx_exl = self._idx_bgl + int(self.dur_leading_bg/self.base_binsize)  # Leading exclusion
        self._idx_sw = self._idx_exl + int(self.dur_leading_excl/self.base_binsize)  # Search window
        self._idx_ext = self._idx_sw + int(self._binsize/self.base_binsize)  # Trailing exclusion
        self._idx_bgt = self._idx_ext + int(self.dur_trailing_excl/self.base_binsize)  # Trailing background

        # Quantities used to construct trigger
        self.hit_sum = np.zeros(self._ndom, dtype=dtype)
        self.hit_sum2 = np.zeros(self._ndom, dtype=dtype)
        self.rate = np.zeros(self._ndom, dtype=dtype)
        self.eps = eps  # Relative efficiency  # TODO: Investigate shared memory so new instances aren't created


    @property
    def signal(self):
        # TODO: Implement this
        raise NotImplementedError

    @property
    def mean(self):
        # TODO: Implement this
        raise NotImplementedError

    @property
    def std(self):
        # TODO: Implement this
        raise NotImplementedError

    @property
    def var(self):
        # TODO: Implement this
        raise NotImplementedError

    @property
    def binsize(self):
        return self._binsize

    @property
    def offset(self):
        return self._offset

    @property
    def duration(self):
        """
        :return: Duration of buffer including background, search window, and exclusion blocks in ms
        :rtype: int
        """
        return self.duration_nosearch + self._binsize

    @property
    def idx_bgl(self):
        """ Index of first column in leading background search window
        :return: _idx_bgl
        :rtype: int
        """
        return self._idx_bgl

    @property
    def idx_exl(self):
        """ Index of first column in leading exclusion window
        :return: _idx_exl
        :rtype: int
        """
        return self._idx_exl

    @property
    def idx_sw(self):
        """ Index of first column in search window
        :return: _idx_sw
        :rtype: int
        """
        return self._idx_sw

    @property
    def idx_ext(self):
        """ Index of first column in trailing exclusion window
        :return: _idx_ext
        :rtype: int
        """
        return self._idx_ext

    @property
    def idx_bgt(self):
        """ Index of first column in trailing background window
        :return: _idx_bgt
        :rtype: int
        """
        return self._idx_bgt


class AnalysisBuffer:

    @property
    def _background(self):
        """
        Background block duration. This enforces specific, constant search parameters
        :return: Duration of sliding background windows in ms
        """
        return 20000

    @property
    def _exclusion(self):
        """
        Exclusion block duration. This enforces specific, constant search parameters
        :return: Duration of sliding exclusion windows in ms
        """
        return 5000

    @property
    def _binnings(self):
        """
        Search window durations. This enforces specific, constant search parameters
        :return: Duration of background windows in ms
        """
        return [500, 1500]  # Used later to initialize search window, variable name might be confusing

    def __init__(self, ndom=5160, dtype=np.uint16):
        """
        Assuming the following parameters
            - Search window is 0.5s         (250 * 2ms,         1 * 500ms)
            - Exclusion blocks are 5s       (2 * 2500 * 2ms,    2 * 10 * 500ms)
            - Background blocks are 20      (2 * 10000 * 2ms,   2 * 40 * 500ms)
            - No offset
        Total Duration: 50.5s               (25250 * 2ms,       101 * 500ms)
        Total Size: 25250
        """
        self.buffer_2ms = windowbuffer(int(self.duration/2))
        self.buffer_500ms = windowbuffer(int(self.duration/500))  # Not a fan of this method of casting
        self._accum_count = 250
        self.runtime = 0
        self.search_windows = []
        self.searches = []
        for binning in self._binnings:
            for offset in np.arange(0, binning, 500):
                self.search_windows.append(
                    Sni3SearchWindow(self.buflen_500ms, self._background, self._exclusion, binning, offset)
                )
                self.searches.append(Sni3SearchStatistics(binning, offset, ndom, dtype))

    @property
    def duration(self):
        """
        :return: Data buffer duration in ms
        :rtype: int
        """
        return 2 * (self._background + self._exclusion + np.max(self._binnings))

    @property
    def buflen_2ms(self):
        """
        :return: Size of 2ms buffer
        :rtype: int
        """
        return self.buffer_2ms.data.shape[0]

    @property
    def buflen_500ms(self):
        """
        :return: Size of 500ms buffer
        :rtype: int
        """
        return self.buffer_500ms.data.shape[0]

    def update(self, val):
        """
        Given values with shape (ndoms, ), assumed to be 2ms-binned data, Performs the following
        2ms BUFFER
            - Updates with new data column
        500ms BUFFER
            - Accumulates 2ms data until 500ms have elapsed
            THEN
            - Updates with new data column
            - Resets accumulator back to zero for new 2ms accumulation
            - Update searches located in self.searches when 500ms buffer updates
        Later:  Update state of buffers IE (Empty, Waiting, Startup, Normal)
                Perform checking on data type
                DOM Qualification
        :param val: Assumed 2ms data with shape (ndoms, 1)
        :return:
        """
        self.buffer_2ms.append(val)
        self._accum_count -= 1
        if not self._accum_count:
            self._accum_count = 250
            self.runtime += 500
            self.buffer_500ms.append(np.sum(self.buffer_2ms.data[-self._accum_count:], axis=0))

            for search, window in zip(self.searches, self.search_windows):

                add_to_bg = self.buffer_500ms[window.idx_exl] + self.buffer_500ms[window.idx_eof]
                sub_from_bg = self.buffer_500ms[window.idx_bgl] + self.buffer_500ms[window.idx_bgr]

                search.sum_hits += add_to_bg - sub_from_bg
                search.sum_hits2 += add_to_bg**2 - sub_from_bg**2
                search.sum_hits3 += add_to_bg**3 - sub_from_bg**3

                add_to_sw = self.buffer_500ms[window.idx_exr]
                sub_from_sw = self.buffer_500ms[window.idx_sw]

                search.rate += add_to_sw - sub_from_sw


class Sni3SearchWindow:
    """
    :ivar duration: Duration of entire search window, including background and exclusion blocks, in ms
    :ivar idx_bgl: Index at left side of left background block
    :ivar idx_exl: Index at left side of left exclusion block
    :ivar idx_sw:  Index at left side of search window
    :ivar idx_exr: Index at left side of right exclusion block
    :ivar idx_bgr: Index at left side of right background block
    :ivar idx_eof: Index after right side of right background block (EOF)
    """
    def __init__(self, buflen, background, exclusion, search, binning=500, offset=0):
        """
        :param background: Duration of background blocks in ms
        :param exclusion: Duration of exclusion blocks in ms
        :param search: Duration of search window in ms
        :param binning: Binning of data buffer in ms
        :param offset: Offset from beginning of data buffer in ms
        """
        self._buflen = buflen
        self._background = background
        self._exclusion = exclusion
        self._search = search
        self._binning = binning
        self._offset = offset

    @property
    def duration(self):
        """
        :return: Duration of search window including background and exclusion blocks in ms
        :rtype: int
        """
        return 2 * (self._background + self._exclusion) + self._search

    @property
    def idx_bgl(self):
        return int(self._buflen - (self.duration + self._offset)/self._binning) - 1

    @property
    def idx_exl(self):
        return self.idx_bgl + int(self._background/self._binning)

    @property
    def idx_sw(self):
        return self.idx_exl + int(self._exclusion/self._binning)

    @property
    def idx_exr(self):
        return self.idx_sw + int(self._search/self._binning)

    @property
    def idx_bgr(self):
        return self.idx_exr + int(self._exclusion/self._binning)

    @property
    def idx_eof(self):
        return self.idx_bgr + int(self._background/self._binning)


class Sni3SearchStatistics:
    """
    Class to perform search of given binning
    Must be able to perform the following actions
        - Update Signal and background rate totals when new data point is available
            - This requires access to data buffer of parent class in order to set up boundaries of search, exclusion and
                background blocks.
            - It would be preferable to pass out indices of appropriate boundaries to the parent class, which passes
                back the appropriate values, rather than passing a copy of the matrix

    """
    def __init__(self, binning, offset=0, ndoms=5160, dtype=np.uint16):
        """
        :param binning:
        :param offset:
        :param ndoms:
        :param dtype:
        """

        self._binning = binning
        self._offset = offset

        self.rate = np.zeros(ndoms, dtype=dtype)        # Rate within search window
        self.sum_hits = np.zeros(ndoms, dtype=dtype)    # Sum of background rates
        self.sum_hits2 = np.zeros(ndoms, dtype=dtype)   # Sum Squared background Rates
        self.sum_hits3 = np.zeros(ndoms, dtype=dtype)   # Sum Cubed Background Rates

        self.mean = np.zeros(ndoms, dtype=dtype)        # Mean of background rates (Rewrite as function?)
        self.std = np.zeros(ndoms, dtype=dtype)         # St. Dev. of background rates (Rewrite as function?)
        self.sig = np.zeros(ndoms, dtype=dtype)         # Significance (Rewrite as function?)