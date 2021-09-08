"""Analysis objects for performing SNDAQ SICO analysis
"""
import numpy as np
from sndaq.buffer import windowbuffer


class AnalysisConfig:
    """Configuration object used to configure SICO analysis objects
    """
    # IMPORTANT NOTE: Leading/trailing refers to time, buffer indices will be inversely prop to time
    # TODO: Documentation (Figure) or fix to Buffer indexing needed
    _raw_binsize = 2  # ms
    _base_binsize = 500  # ms
    _dur_leading_bg = int(300e3)  # ms
    _dur_trailing_bg = int(300e3)  # ms
    _dur_leading_excl = int(15e3)  # ms
    _dur_trailing_excl = int(15e3)  # ms

    def __init__(self, raw_binsize=None, base_binsize=None, dur_bgl=None, dur_bgt=None, dur_exl=None, dur_ext=None):
        """

        Parameters
        ----------
        raw_binsize : int
            Size of input scalar data time bins
        base_binsize : int
            Size of base analysis time bins in ms
        dur_bgl :
            Duration of leading background window in ms
        dur_bgt :
            Duration of trailing background window in ms
        dur_exl :
            Duration of leading exclusion window in ms
        dur_ext :
            Duration of trailing exclusion window in ms
        """
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
        """Duration of analysis window in ms, excluding search window

        Returns
        -------
        duration : int
            Duration of analysis window including background and exclusion blocks in ms
        """
        return self.dur_leading_bg + self.dur_leading_excl + self.dur_trailing_bg + self.dur_trailing_excl

    @property
    def raw_binsize(self):
        """Input data binsize in ms

        Returns
        -------
        binsize : int
            Size of input scalar data time in ms
        """
        return self._raw_binsize

    @property
    def base_binsize(self):
        """Base analysis binsize in ms

        Returns
        -------
        binsize: int
            Size of base analysis time bins in ms
        """
        return self._base_binsize

    @property
    def dur_leading_bg(self):
        """Leading background duration in ms

        Returns
        -------
        duration : int
            Duration of leading background window in ms
        """
        return self._dur_leading_bg

    @property
    def dur_trailing_bg(self):
        """Trailing background duration in ms

        Returns
        -------
        duration : int
            Duration of trailing background window in ms
        """
        return self._dur_trailing_bg

    @property
    def dur_leading_excl(self):
        """Leading exclusion duration in ms

        Returns
        -------
        duration : int
            Duration of leading exclusion window in ms
        """
        return self._dur_leading_excl

    @property
    def dur_trailing_excl(self):
        """Leading exclusion duration in ms

        Returns
        -------
        duration : int
            Duration of trailing exclusion window in ms
        """
        return self._dur_trailing_excl


class AnalysisHandler(AnalysisConfig):
    """Container for Analysis objects and functions for use in SNDAQ's SICO search algorithm.
    """
    def __init__(self, binnings=(500, 1.5e3, 4e3, 10e3), ndom=5160, eps=np.ones(5160, dtype=float), dtype=np.uint16):
        """Create Analysis Handler

        Parameters
        ----------
        binnings : array_like
            Bin sizes to be used during SICO analysis
        ndom : int
            number of contributing DOMs
        eps : numpy.ndarray
            relative efficiency of contributing DOMs
        dtype
            Data type for SN scaler arrays
        """
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
        """DOM Relative efficiency

        Returns
        -------
        eps : numpy.ndarray
            ndom-length array of floats describing relative efficiency of each DOM.

        Notes
        -----
        It is assumed that the ordering of DOM in eps matches the order of DOMs in the data buffer
        """
        return self._eps

    def print_analyses(self):
        """Print binsize and relative offset of all Analysis object
        """
        for i, analysis in enumerate(self.analyses):
            print(f'{i:d} {analysis.binsize*1e-3:4.1f} (+{analysis.offset*1e-3:4.1f})')

    def update_analyses(self, value):
        """Update SICO sums and computed quantities for all analyses

        Parameters
        ----------
        value : numpy.ndarray
            ndom-length array of binned hits to be added to SICO analysis sums
        """
        for analysis in self.analyses:
            self.update_sums(analysis, value)
            if self.istriggerable(analysis):
                self.update_results(analysis)

    def istriggerable(self, analysis):
        """Indicates analysis is ready to trigger

        Parameters
        ----------
        analysis : sndaq.analysis.Analysis
            Analysis object for which to check triggering status. An analysis will (currently) only issue triggers if
            its background windows have filled with data.

        Returns
        -------
        istriggerable : bool
            If true, the background buffers have filled and analysis is issuing triggers.
            If False, the background buffer have not yet filled.
        """
        return self._n >= analysis.n_to_trigger

    def update_sums(self, analysis, value):
        """Update SICO analysis sums

        Parameters
        ----------
        analysis : sndaq.analysis.Analysis
            Analysis object for which to update sums
        value : numpy.ndarray
            ndom-length array of binned hits to be added to SICO analysis sums
        """
        # Analysis sums are updated by the handler b/c the handler has access to buffers whereas analysis objects do not
        # TODO: May want to add check eventually if asymmetric bg/excl window is used
        analysis.hit_sum += self.buffer_analysis[analysis.idx_exl] + value
        analysis.hit_sum -= self.buffer_analysis[analysis.idx_bgl] + self.buffer_analysis[analysis.idx_bgt]

        analysis.hit_sum2 += self.buffer_analysis[analysis.idx_exl]**2 + value**2
        analysis.hit_sum2 -= self.buffer_analysis[analysis.idx_bgl]**2 + self.buffer_analysis[analysis.idx_bgt]**2

        analysis.rate += self.buffer_analysis[analysis.idx_ext]
        analysis.rate -= self.buffer_analysis[analysis.idx_sw]

    def update_results(self, analysis):
        """Update SICO analysis results

        Parameters
        ----------
        analysis : sndaq.analysis.Analysis
            Analysis object for which to update quantities computed from sums
        """
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

    def accumulate(self, val):
        """Accumulate 2 ms data into analysis binsize

        Parameters
        ----------
        val : numpy.ndarray
            ndom-length array of 2 ms scaler hits to be accumulated into higher order binnings.

        Returns
        -------
        continue_accum : bool
            Indicates that accumulation should continue. When false, an analysis' binsize worth of 2ms data has
            accumulated and is ready to be written to file.

        Notes
        -----
        When this function returns false, it is expected that reset_accumulator will be called before this function is
        called again.

        See Also
        --------
        sndaq.analysis.reset_accumulator
        """
        # This could be it's own class/component, maybe use itertools? Maybe use a generator w/ yield?
        self._accum_data += val
        self._accum_count -= 1
        return bool(self._accum_count)

    def reset_accumulator(self):
        """Reset accumulator count to rebin_factor and accum_data to zeros

        Notes
        -----
        This function is expected to be called as soon as an analysis' binsize worth of 2ms data has accumulated

        See Also
        --------
        sndaq.analysis.accumulate
        """
        self._accum_data = np.zeros(self._ndom, dtype=self._dtype)
        # _ndom and _dtype could be removed if this was part of an accumulator object, defined during init.
        self._accum_count = self._rebin_factor

    def update(self, value):
        """Update raw buffer, analysis buffer, analyses sums and analysis results

        Parameters
        ----------
        value : numpy.ndarray
            2ms data for each DOM at a particular timestamp
        """
        self.buffer_raw.append(value)
        if not self.accumulate(value):  # Accumulator indicates time to reset, as base analysis bin of data is ready
            # There's almost certainly a better way to do this.
            accumulated_data = np.asarray(self._accum_data, dtype=np.uint16)
            self.reset_accumulator()
            if not self.buffer_analysis.filled:
                self._n += 1
            self.update_analyses(accumulated_data)
            self.buffer_analysis.append(accumulated_data)

    # TODO: Move this to Alert handler
    def check_for_triggers(self, threshold=8.4, corr_threshold=5.8):
        """Check if any analysis meets the basic trigger condition.

        Parameters
        ----------
        threshold : float
            Uncorrected xi threshold for issuing a SN trigger alert
        corr_threshold : float
            Corrected xi threshold for issuing a SN trigger alert
        """
        # Probably out of intended scope for analysis object
        xi = np.array((ana.xi for ana in self.analyses))
        if np.any(xi > threshold) or np.any(xi > corr_threshold):
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
    """Descriptor object to handle data access and algorithms for SNDAQ sico-analysis

    """
    def __init__(self, binsize, offset, idx=0, ndom=5160, dtype=np.uint16):
        """Create Analysis object

        Parameters
        ----------
        binsize : int
            Time size of bins in signal rate and background rate calculation
        offset : int
            Time offset of analysis window in ms
        idx : int
            Starting index in analysis buffer, includes time offset
        ndom : int
            Number of DOMs contributing to the analysis
        dtype
            Data type for SN scaler arrays
        """
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
        """Number of background and exclusion bins

        Returns
        -------
        nbin_nosearch : int
            Number of time bins within both background and exclusions windows (All bins other than search window)
        """
        return self._nbin_nosearch

    @property
    def nbin_bg(self):
        """Number of background bins

        Returns
        -------
        nbin_background : int
            Number of time bins within both background windows
        """
        return self._nbin_background

    @property
    def signal(self):
        """Signal rate

        Returns
        -------
        signal : np.ndarray(float)
            Deviation of rate in search window from mean background rate
        """
        return self.rate - self.mean

    @property
    def mean(self):
        """Background rate mean

        Returns
        -------
        mean : float
            Mean of background hit rate per bin measured across both background windows
        """
        # TODO: Unit test for float type!
        return self.hit_sum / self.nbin_bg

    @property
    def var(self):
        """Background rate variance

        Returns
        -------
        variance : float
            Variance of background hit rate per bin measured across both background windows
        """
        # TODO: Unit test for float type!
        return (self.nbin_bg * self.hit_sum2 - self.hit_sum**2) / self.nbin_bg**2

    @property
    def std(self):
        """Background rate standard deviation

        Returns
        -------
        std : float
            Standard deviation of background hit rate pre bin measured across both background windows
        """
        # TODO: Unit test for float type!
        return np.sqrt(self.var)

    @property
    def binsize(self):
        """Analysis binsize in ms

        Returns
        -------
        _binsize : int
            Size of analysis bins in search and background window
        """
        return self._binsize

    @property
    def offset(self):
        """Analysis window offset in ms

        Returns
        -------
        _offset : int
            Time offset of analysis window in ms, by default in increments of 500 ms
        """
        return self._offset

    @property
    def duration(self):
        """Duration of analysis window in ms
        Returns
        -------
        duration : int
            Duration of analysis window in ms, comprised of background, exclusion, and search window
        """
        return self.duration_nosearch + self._binsize

    @property
    def idx_bgl(self):
        """Leading background window index

        Returns
        -------
        _idx_bgl : int
            Index of first column in leading background window
        """
        return self._idx_bgl

    @property
    def idx_exl(self):
        """Leading exclusion window index

        Returns
        -------
        _idx_exl : int
            Index of first column in leading exclusion window
        """
        return self._idx_exl

    @property
    def idx_sw(self):
        """Search window index

        Returns
        -------
        _idx_sw : int
            Index of first column in search window
        """
        return self._idx_sw

    @property
    def idx_ext(self):
        """Trailing exclusion window index

        Returns
        -------
        _idx_ext : int
            Index of first column in trailing exclusion window
        """
        return self._idx_ext

    @property
    def idx_bgt(self):
        """Trailing background window index

        Returns
        -------
        _idx_bgt
        Index of first column in trailing background window
        """
        return self._idx_bgt
