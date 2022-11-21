"""Analysis objects for performing SNDAQ SICO analysis
"""
import numpy as np
from sndaq.buffer import windowbuffer
from sndaq.trigger import BasicTrigger, Trigger


class AnalysisConfig:
    """Configuration object used to configure SICO analysis objects
    """
    # IMPORTANT NOTE: Leading/trailing refers to time, buffer indices will be inversely prop to time
    # TODO: Documentation (Figure) or fix to Buffer indexing needed
    _raw_binsize = 2  # ms
    _base_binsize = 500  # ms
    _dur_leading_bg = int(300e3)  # ms
    _dur_trailing_bg = int(300e3)  # ms
    _dur_leading_excl = int(30e3)  # ms
    _dur_trailing_excl = int(30e3)  # ms
    # _dur_leading_excl = int(15e3)  # ms
    # _dur_trailing_excl = int(15e3)  # ms
    _dur_trigger_window = int(30e3)
    _trigger_level = BasicTrigger
    # _trigger_level.threshold = 5.8

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

    Methods
    -------
    accumulate:
        Accumulate 2 ms data into analysis bin size
    check_for_triggers:
        Check if any analysis meets the basic trigger condition.
    istriggerable:
        Indicates analysis is ready to trigger
    print_analyses:
        Print binsize and relative offset of all analysis objects
    reset_accumulator:
        Reset accumulator count to rebin_factor and accum_data to zeros
    update:
        Update raw buffer, analysis buffer, analyses sums and analysis results
    update_analyses:
        Update SICO sums and computed quantities for all analyses
    update_results:
        Update SICO analysis results
    update_sums:
        Update SICO analysis sums

    """
    def __init__(self, binnings=(500, 1.5e3, 4e3, 10e3), ndom=5160, eps=None, dtype=np.uint16,
                 starttime=0, dropped_doms=None):
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
        if eps is None:
            self._eps = np.where(np.arange(5160) > 4800, np.ones(5160), 1.35*np.ones(5160))
        else:
            self._eps = eps
        self._dtype = dtype
        self._starttime = starttime

        if dropped_doms is not None:
            self._eps = np.delete(self._eps, dropped_doms, axis=0)
            self._ndom -= dropped_doms.size
            # TODO: check for compatibility between self._ndom and ndom argument when dropped doms are present

        # Size is computed so the following may be included in buffer
        #   Leading/trailing background and exclusion windows, and search window (duration_nosearch + max(binnings))
        #   Max analysis offset (max(binnings) - base_binsize)
        #   Rates to subtract from buffer during analysis (max(binnings))
        self._size = ((self.duration_nosearch + 3*int(max(binnings))) // self.base_binsize) - 1
        self.buffer_analysis = windowbuffer(size=self._size, ndom=self._ndom, dtype=np.uint64)
        self._rebin_factor = int(self.base_binsize/self.raw_binsize)
        self.buffer_raw = windowbuffer(size=self._size*self._rebin_factor, ndom=ndom, dtype=dtype)

        # Create analyses
        self.analyses = []
        for binning in np.asarray(binnings, dtype=dtype):
            for offset in np.arange(0, binning, 500, dtype=dtype):
                idx = int(self._size - (self.duration_nosearch + offset + binning)/self.base_binsize)
                self.analyses.append(
                    Analysis(binning, offset, idx=idx, ndom=self._ndom)
                )

        # Define counter for accumulation used in rebinning from raw to base analysis
        self._accum_count = self._rebin_factor
        self._accum_data = np.zeros(ndom, dtype=dtype)

        # Define trigger handler
        # TODO: Move to Alert Handler
        self.current_trigger = Trigger()
        # self.trigger_pending = False
        self.trigger_xi = 0.
        self.triggered_analysis = None
        self._n_bins_trigger_window = int(self._dur_trigger_window/self.base_binsize)

    @property
    def trigger_pending(self):
        return self.buffer_analysis.n <= self._n_trigger_close

    @trigger_pending.setter
    def trigger_pending(self, value):
        # TODO: Make this functionality into a "extend window" function
        # This may be a horribly counter-intuitive way of doing this...
        # The intent is to be able to easily reset the trigger_pending condition without having to manage any counters
        if isinstance(value, bool):
            if value:
                self._n_trigger_close = self.buffer_analysis.n + self._n_bins_trigger_window

    @property
    def ndom(self):
        return self._ndom

    @property
    def current_time(self):
        return self._starttime + self.buffer_analysis.n * 0.5

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
        """Print binsize and relative offset of all analysis objects
        """
        for i, analysis in enumerate(self.analyses):
            print(f'{i:d} {analysis.binsize*1e-3:4.1f} (+{analysis.offset*1e-3:4.1f})')

    def update_analyses(self):
        """Update SICO sums and computed quantities for all analyses

        Parameters
        ----------
        value : numpy.ndarray
            ndom-length array of binned hits to be added to SICO analysis sums
        """
        for analysis in self.analyses:
            self.update_sums(analysis)

            if analysis.is_updatable:
                if analysis.is_online:
                    self.update_results(analysis)
                analysis.reset_accum()  # Reset "updatable" counter TODO: Rename this to be more consistent

    def update_sums(self, analysis):  # Assumes call after value has been appended to buffer
        """Update SICO analysis sums after new data has been added to the buffer

        Parameters
        ----------
        analysis : sndaq.analysis.Analysis
            Analysis object for which to update sums
        """
        # IMPORTANT!! ASSUMES VALUES ARE APPENDED TO BUFFER **BEFORE** `update_sums` IS CALLED!!
        analysis.n_accum += 1
        if not analysis.is_online:
            analysis.n += 1  # Update until analysis.is_online returns true

        if analysis.is_updatable:
            # TODO: Find better names for these
            add_to_bgl = self.buffer_analysis[analysis.idx_addbgl].sum(axis=0)
            sub_from_bgl = self.buffer_analysis[analysis.idx_subbgl].sum(axis=0)

            add_to_bgt = self.buffer_analysis[analysis.idx_addbgt].sum(axis=0)
            sub_from_bgt = self.buffer_analysis[analysis.idx_subbgt].sum(axis=0)

            add_to_sw = self.buffer_analysis[analysis.idx_addsw].sum(axis=0)
            sub_from_sw = self.buffer_analysis[analysis.idx_subsw].sum(axis=0)

            analysis.rate += add_to_sw
            analysis.rate -= sub_from_sw

            analysis.hit_sum += add_to_bgl + add_to_bgt
            analysis.hit_sum -= (sub_from_bgl + sub_from_bgt)

            analysis.hit_sum2 += (add_to_bgl**2 + add_to_bgt**2)
            analysis.hit_sum2 -= (sub_from_bgl**2 + sub_from_bgt**2)
            analysis.n_accum = 0

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

    def accumulate(self, val, idx):
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
        np.add.at(self._accum_data, idx, val)
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
        idx = value.nonzero()[0]
        if not self.accumulate(value[idx], idx):  # Accumulator indicates time to reset, as base analysis bin of data is ready
            # There's almost certainly a better way to do this.
            accumulated_data = np.asarray(self._accum_data, dtype=np.uint16)
            self.reset_accumulator()
            # TODO: Remove _n
            # if not self.buffer_analysis.filled:
            #     self._n += 1
            self.update_analyses(accumulated_data)
            self.buffer_analysis.append(accumulated_data)

    def process_triggers(self):
        """Check if any analysis meets the basic trigger condition.

        """
        # Probably out of intended scope for analysis object
        xi = np.array([ana.xi if (ana.is_online and ana.is_triggerable) else 0
                       for ana in self.analyses])
        xi_max = xi.max()
        # TODO: Figure out how to optimize this with ana.is_online/is_triggerable - there's a lot of wasted time here
        # This is built to execute every time the analysis buffer updates, maybe this could be called when the analyses
        # are updated.
        # Muon Correction is not implemented yet, should this be performed *after* highest uncorr trigger is found?

        if xi_max >= self._trigger_level.threshold and xi_max > self.current_trigger.xi:
            idx = xi.argmax()
            ana = self.analyses[idx]
            t = (self.buffer_analysis.n - ana.n_to_trigger) * 0.5
            self.trigger_pending = True
            self.current_trigger = Trigger(xi=ana.xi, xi_corr=0, t=t, binsize=ana.binsize, offset=ana.offset)

    @property
    def trigger_finalized(self):
        return self.current_trigger.xi > 0 and not self.trigger_pending


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
        if (binsize % self.base_binsize) > 0:  # Binsize must be an integer multiple of base_binsize
            raise RuntimeError(f'Binsize {binsize:d} ms is incompatible, must be factor of {self.base_binsize:d} ms')
        self._binsize = binsize  # ms
        self._offset = offset  # ms
        self._rebin_factor = int(self._binsize / self.base_binsize)
        # TODO: Decide if ndom should always be 5160 or the number of doms in the current config
        self._ndom = ndom

        self._nbin_nosearch = self.duration_nosearch / self._binsize
        self._nbin_background = (self._dur_leading_bg + self._dur_trailing_bg) / self._binsize

        # Indices for accessing data buffer, all point to first column in respective region
        # TODO: Check alignment so all start filling as soon as possible
        self._idx_bgt = idx  # Trailing background
        self._idx_ext = self._idx_bgt + int(self.dur_trailing_bg/self.base_binsize)  # Trailing exclusion
        self._idx_sw = self._idx_ext + int(self.dur_trailing_excl/self.base_binsize)  # Search window
        self._idx_exl = self._idx_sw + int(self._binsize/self.base_binsize)  # Leading exclusion
        self._idx_bgl = self._idx_exl + int(self.dur_leading_excl/self.base_binsize)  # Leading background
        self.idx_eod = self._idx_bgl + int(self.dur_leading_bg/self.base_binsize)  # End of data in analysis

        # Indices of buffer for "bins" to add to sums for analysis
        # Using np.arange here (np arrays as indices) allows all analyses to be indexed in the same way
        # It's important to compute this only once, as these indices will never change for a given analysis
        self._idx_addbgl = np.arange(self.idx_eod-self.rebin_factor, self.idx_eod)  # Add to bgl
        self._idx_subbgl = np.arange(self.idx_bgl-self.rebin_factor, self.idx_bgl)  #
        self._idx_addbgt = np.arange(self.idx_ext-self.rebin_factor, self.idx_ext)
        self._idx_subbgt = np.arange(self.idx_bgt-self.rebin_factor, self.idx_bgt)
        self._idx_addsw = np.arange(self.idx_exl-self.rebin_factor, self.idx_exl)
        self._idx_subsw = np.arange(self.idx_sw-self.rebin_factor, self.idx_sw)

        # Quantities used to construct trigger
        self.hit_sum = np.zeros(self._ndom, dtype=np.uint64)
        self.hit_sum2 = np.zeros(self._ndom, dtype=np.uint64)
        self.rate = np.zeros(self._ndom, dtype=np.uint64)
        self.n_accum = 0

        # Quantities used to evaluate trigger
        self.dmu = 0.
        self.var_dmu = 0.
        self.xi = 0.
        self.chi2 = 0.

        # Quantities used to evaluate when analysis is ready to start forming sums and issuing triggers
        # Analysis becomes triggerable when trailing background has filled
        self.n_to_trigger = self.idx_eod - self.idx_bgt + int(self.offset / self.base_binsize)
        self.n = 0

    def reset_accum(self):
        self.n_accum = 0

    @property
    def is_online(self):
        """Indicates whether analysis has received enough data to form triggers (both bkg buffers have filled)
        This prevents triggering before an estimate of the background rate can be formed

        Returns
        -------
        is_online : bool
            If true, the background buffers have filled and analysis ready to issue triggers.
            If False, the background buffer have not yet filled.
        """
        return self.n >= self.n_to_trigger

    @property
    def is_updatable(self):
        """Indicates whether analysis has received enough data to update its analysis quantities

        Returns
        -------
        is_online : bool
            If true, enough data has been received, and the analysis quantities are ready to be updated
            If False, the background buffer have not yet filled.
        """
        return self.n_accum == self.rebin_factor

    @property
    def is_triggerable(self):
        """Indicates whether analysis has received enough data to fill the current time bin

        Returns
        -------
        is_online : bool
            If true,
            If False, less than `self._binsize` data has been received
        """
        return self.n_accum == 0

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
        return ((self.nbin_bg * self.hit_sum2) - (self.hit_sum**2)) / self.nbin_bg**2

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
    def rebin_factor(self):
        """Rebinning factor (Ratio of binsize to base_binsize)

        Returns
        -------
        rebin_factor: int
            Rebinning factor
        """
        return self._rebin_factor

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

    @property
    def idx_addbgl(self):
        """Indices to add to leading background during binned analysis
        """
        return self._idx_addbgl

    @property
    def idx_subbgl(self):
        """Indices to subtract from leading background during binned analysis
        """
        return self._idx_subbgl

    @property
    def idx_addbgt(self):
        """Indices to add to trailing background during binned analysis
        """
        return self._idx_addbgt

    @property
    def idx_subbgt(self):
        """Indices to subtract from trailing background during binned analysis
        """
        return self._idx_subbgt

    @property
    def idx_addsw(self):
        """Indices to add to search window during binned analysis
        """
        return self._idx_addsw

    @property
    def idx_subsw(self):
        """Indices to subtract from search window during binned analysis
        """
        return self._idx_subsw
