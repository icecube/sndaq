import numpy as np
from sndaq.buffer import windowbuffer


class AnalysisConfig:

    # IMPORTANT NOTE: Leading/trailing refers to time, buffer indices will be inversely prop to time
    # TODO: Documentation (Figure) or fix to Buffer indexing needed
    _raw_binsize = 2  # ms
    _base_binsize = 500  # ms
    _dur_leading_bg = int(300e3)  # ms
    _dur_trailing_bg = int(300e3)  # ms
    _dur_leading_excl = int(15e3)  # ms
    _dur_trailing_excl = int(15e3)  # ms

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

    def __init__(self, binnings=(500, 1.5e3, 4e3, 10e3), ndom=5160, eps=np.ones(5160, dtype=float), dtype=np.uint16):
        super().__init__()

        # Create shared window buffer
        self._binnings = binnings
        self._ndom = ndom
        self._eps = eps
        self._dtype = dtype

        # minimum size for bg, excl, search, and largest search offset
        self._size = ((self.duration_nosearch + 2*int(max(binnings))) // self.base_binsize) - 1
        self.buffer_analysis = windowbuffer(size=self._size, ndom=ndom, dtype=dtype)
        self._rebin_factor = int(self.base_binsize/self.raw_binsize)
        self.buffer_raw = windowbuffer(size=self._size*self._rebin_factor, ndom=ndom, dtype=dtype)

        # Create analyses
        self.analyses = []
        for binning in np.asarray(binnings, dtype=dtype):
            for offset in np.arange(0, binning, 500, dtype=dtype):
                idx = int(self._size - (self.duration_nosearch + offset + binning)/self.base_binsize)
                self.analyses.append(
                    Analysis(binning, offset, idx=idx, ndom=ndom)
                )

        # Define counter for accumulation used in rebinning from raw to base analysis
        self._accum_count = self._rebin_factor
        self._accum_data = np.zeros(ndom, dtype=dtype)

        # Define trigger handler
        self._n = 0
        # TODO: Move to Alert Handler
        self.trigger_pending = False
        self.trigger_xi = 0.
        self.triggered_analysis = None

    @property
    def eps(self):
        return self._eps

    def print_analyses(self):
        for i, analysis in enumerate(self.analyses):
            print(f'{i:d} {analysis.binsize*1e-3:4.1f} (+{analysis.offset*1e-3:4.1f})')

    def update_analyses(self, value):
        for analysis in self.analyses:
            self.update_sums(analysis, value)
            if self.istriggerable(analysis):
                self.update_results(analysis)

    def istriggerable(self, analysis):
        return self._n >= analysis.n_to_trigger

    def update_sums(self, analysis: 'Analysis', value):
        # Analysis sums are updated by the handler b/c the handler has access to buffers whereas analysis objects do not
        # TODO: May want to add check eventually if asymmetric bg/excl window is used
        analysis.hit_sum += self.buffer_analysis[analysis.idx_exl] + value
        analysis.hit_sum -= self.buffer_analysis[analysis.idx_bgl] + self.buffer_analysis[analysis.idx_bgt]

        analysis.hit_sum2 += self.buffer_analysis[analysis.idx_exl]**2 + value**2
        analysis.hit_sum2 -= self.buffer_analysis[analysis.idx_bgl]**2 + self.buffer_analysis[analysis.idx_bgt]**2

        analysis.rate += self.buffer_analysis[analysis.idx_ext]
        analysis.rate -= self.buffer_analysis[analysis.idx_sw]

    def update_results(self, analysis: 'Analysis'):
        # Analysis results are updated by the handler as the analysis class (currently) is intended to be a container
        #   The handler is intended to contain the algorithms
        mean = analysis.mean
        var = analysis.var
        rate = analysis.rate
        signal = rate - mean

        sum_rate_dev = np.sum(signal * self.eps / var)
        sum_inv_var = np.sum(self.eps**2 / var)
        analysis.dmu = sum_rate_dev / sum_inv_var
        analysis.var_dmu = 1. / sum_inv_var

        # calc xi
        analysis.xi = analysis.dmu / np.sqrt(analysis.var_dmu)

        # calc chi2
        # tmp = (signal*(1. - eps))**2 / (var + eps*abs(signal))
        analysis.chi2 = np.sum((rate - (mean+self.eps*signal))**2 / (var + self.eps*abs(signal)))

    def accumulate(self, val, idx):
        # This could be it's own class/component, maybe use itertools? Maybe use a generator w/ yield?
        np.add.at(self._accum_data, idx, val)
        self._accum_count -= 1
        return bool(self._accum_count)

    def reset_accumulator(self):
        self._accum_data = np.zeros(self._ndom, dtype=self._dtype)
        # _ndom and _dtype could be removed if this was part of an accumulator object, defined during init.
        self._accum_count = self._rebin_factor

    def update(self, value):
        """ Update raw buffer, analysis buffer, analyses sums and analysis results
        :param value: 2ms data for each DOM at a particular timestamp
        :type value: np.ndarray
        :return: None
        :rtype: None
        """
        self.buffer_raw.append(value)
        idx = value.nonzero()[0]
        if not self.accumulate(value[idx], idx):  # Accumulator indicates time to reset, as base analysis bin of data is ready
            # There's almost certainly a better way to do this.
            accumulated_data = np.asarray(self._accum_data, dtype=np.uint16)
            self.reset_accumulator()
            if not self.buffer_analysis.filled:
                self._n += 1
            self.update_analyses(accumulated_data)
            self.buffer_analysis.append(accumulated_data)

    # TODO: Move this to Alert handler
    def check_for_triggers(self, threshold=8.4, corr_threshold=5.8):
        # Probably out of intended scope for analysis object
        xi = np.array((ana.xi for ana in self.analyses))
        if np.any(xi > threshold):
            if not self.trigger_pending:
                self.trigger_pending = True

            # Check for other triggers in other search windows
            # TODO: Figure out how SNDAQ checks for triggers in other search windows
            if xi.max > self.trigger_xi:
                self.trigger_xi = xi.max
                self.triggered_analysis = self.analyses[xi.argmax()]

        # Issue alert (Definitely out of intended scope)
        # Reset trigger state (Probably out of intended scope)


class Analysis(AnalysisConfig):

    def __init__(self, binsize, offset, idx=0, ndom=5160, dtype=np.uint16):
        super().__init__()
        if binsize % self.base_binsize:
            raise RuntimeError(f'Binsize {binsize:d} ms is incompatible, must be factor of {self.base_binsize:d} ms')
        self._binsize = binsize  # ms
        self._offset = offset  # ms
        self._rebinfactor = self._binsize / self.base_binsize
        # TODO: Decide if ndom should always be 5160 or the number of doms in the current config
        self._ndom = ndom

        self._nbin_nosearch = self.duration_nosearch / self._binsize
        self._nbin_background = (self._dur_leading_bg + self._dur_trailing_bg) / self._binsize

        # Indices for accessing data buffer, all point to first column in respective region
        # TODO: Check alignment so all start filling at the same time, looks like they may stop filling at same time
        self._idx_bgl = idx  # Leading background window
        self._idx_exl = self._idx_bgl + int(self.dur_leading_bg/self.base_binsize)  # Leading exclusion
        self._idx_sw = self._idx_exl + int(self.dur_leading_excl/self.base_binsize)  # Search window
        self._idx_ext = self._idx_sw + int(self._binsize/self.base_binsize)  # Trailing exclusion
        self._idx_bgt = self._idx_ext + int(self.dur_trailing_excl/self.base_binsize)  # Trailing background

        # Quantities used to construct trigger
        self.hit_sum = np.zeros(self._ndom, dtype=dtype)
        self.hit_sum2 = np.zeros(self._ndom, dtype=dtype)
        self.rate = np.zeros(self._ndom, dtype=dtype)

        # Quantities used to evaluate trigger
        self.dmu = 0.
        self.var_dmu = 0.
        self.xi = 0.
        self.chi2 = 0.

        # Quantities used to evaluate when analysis is ready to issue triggers
        # Assumes second BG window to be filled
        self.n_to_trigger = self.idx_bgt + int(self.dur_trailing_bg / self.base_binsize)

    @property
    def nbin_nosearch(self):
        return self._nbin_nosearch

    @property
    def nbin_bg(self):
        return self._nbin_background

    @property
    def signal(self):
        return self.rate - self.mean

    @property
    def mean(self):
        # TODO: Unit test for float type!
        return self.hit_sum / self.nbin_bg

    @property
    def var(self):
        # TODO: Unit test for float type!
        return (self.nbin_bg * self.hit_sum2 - self.hit_sum**2) / self.nbin_bg**2

    @property
    def std(self):
        # TODO: Unit test for float type!
        return np.sqrt(self.var)

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
