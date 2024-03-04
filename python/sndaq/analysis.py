"""Analysis objects for performing SNDAQ SICO analysis
"""
import numpy as np
from configparser import ConfigParser
import ast  # TODO: Replace with pyyaml
from sndaq.buffer import windowbuffer
from sndaq.trigger import PrimaryTrigger, Trigger, FastResponseTrigger
from sndaq.logger import get_logger
from sndaq.util import datetime64_to_utime, utime_to_datetime64

logger = get_logger()

_ana_conf_repr_string = """Analysis Configuration
======================
| 
| Buffer Configuration
| --------------------
| Use Offsets : {use_offsets}
| Use Rebins : {use_rebins}
| Trailing Background (ms): [ -{bgt_shifted_t0}, -{_duration_ext_ms} ]
| Trailing Exclusion (ms): [ -{_duration_ext_ms}, 0 ]
| Search Windows (ms): {_binsize_ms}
| Leading Exclusion (ms): [ t_sw, t_sw + {_duration_exl_ms} ]
| Leading Background (ms): [ t_sw + {_duration_exl_ms}, t_sw + {bgl_shifted_t1} ] 
|
| .. t_sw = search window upper bin edge
| 
| DOM Qualification
| -----------------
| Rate: [{min_bkg_rate}, {max_bkg_rate}]
| Fano Factor: [{min_bkg_fano}, {max_bkg_fano}]
| Abs. Skew < {max_bkg_abs_skew}
======================
"""


class AnalysisConfig:
    """Configuration object used to configure SICO analysis objects
    """
    # IMPORTANT NOTE: Leading/trailing refers to time, buffer indices will be inversely prop to time
    # TODO: Documentation (Figure) or fix to Buffer indexing needed
    _raw_binsize = 2  # ms
    _dur_signi_buffer = int(600e3)  # ms (10 min)
    _dur_trigger_window = int(30e3)
    _trigger_condition = FastResponseTrigger  # PrimaryTrigger

    # _trigger_level.threshold = 5.8

    def __init__(self, use_offsets, use_rebins, binsize_ms,
                 duration_bgl_ms, duration_bgt_ms, duration_exl_ms, duration_ext_ms,
                 min_active_doms=None, min_bkg_rate=None, max_bkg_rate=None,
                 min_bkg_fano=None, max_bkg_fano=None, max_bkg_abs_skew=None):
        """
        Parameters
        ----------
        use_offsets : bool
            Switch to use multiple analyses offset by the smallest binsize
        use_rebins : bool
            Switch to perform analysis on rebinned buffer or base buffer
        binsize_ms : list of int
            Duration(s) of search window(s) in ms at which to perform the analysis
        duration_bgl_ms : int
            Duration of leading background window in ms
        duration_bgt_ms : int
            Duration of trailing background window in ms
        duration_exl_ms : int
            Duration of leading exclusion window in ms
        duration_ext_ms : int
            Duration of trailing exclusion window in ms
        min_active_doms : int
            Mininum number of qualified DOMs required to perform Analysis
        min_bkg_rate : float
            Minimum tolerable DOM background rate
        max_bkg_rate : float
            Maximum tolerable DOM background rate
        min_bkg_fano : float
            Minimum tolerable DOM background Fano factor
        max_bkg_fano : float
            Maximum tolerable DOM background Fano factor
        max_bkg_abs_skew : float
            Maximum tolerable DOM background absolute skew
        """
        self.use_offsets = use_offsets
        self.use_rebins = use_rebins
        self._binsize_ms = np.array(binsize_ms)
        self._duration_bgl_ms = duration_bgl_ms
        self._duration_bgt_ms = duration_bgt_ms
        self._duration_exl_ms = duration_exl_ms
        self._duration_ext_ms = duration_ext_ms
        self.min_active_doms = min_active_doms
        self.min_bkg_rate = min_bkg_rate
        self.max_bkg_rate = max_bkg_rate
        self.min_bkg_fano = min_bkg_fano
        self.max_bkg_fano = max_bkg_fano
        self.max_bkg_abs_skew = max_bkg_abs_skew

        self._base_binsize = np.min(self.binsize_ms)
        self.max_binsize = np.max(self.binsize_ms)  # TODO: Change into property

    def __repr__(self):
        kwargs = vars(self)

        # Add two new keys:
        kwargs.update(bgl_shifted_t1=self.duration_bgl_ms+self.duration_exl_ms,
                      bgt_shifted_t0=self.duration_bgt_ms+self.duration_ext_ms)

        return _ana_conf_repr_string.format(**kwargs)

    @classmethod
    def from_config(cls, conf=None, conf_path=None):
        """Initialize AnalysisConfig from Config or Config file

        Parameters
        ----------
        conf : configparser.ConfigParser
            Analysis configuration
        conf_path :
            Path to file containing analysis configuration
        """
        if conf is None and conf_path is None:
            raise ValueError("Missing configuration")
        elif conf is None and conf_path is not None:
            conf = ConfigParser()
            conf.read(conf_path)

        # TODO Add to base class, have handlers inherit and add class member to handlers for the config key
        conf_dict = {key: ast.literal_eval(val) for key, val in conf['binned_search'].items()}
        try:
            return cls(**conf_dict)
        except TypeError as err:
            msg = str(err)
            bad_field = msg.split('\'')[-2]
            if "required positional argument" in msg:
                raise TypeError(f"Config.: {conf} is missing a required field: '{bad_field}'") from err
            elif "got an unexpected keyword argument" in msg:
                raise TypeError(f"Config.: {conf} contains an unexpected field: '{bad_field}'") from err

    @property
    def duration_nosearch(self):
        """Duration of analysis window in ms, excluding search window

        Returns
        -------
        duration : int
            Duration of analysis window including background and exclusion blocks in ms
        """
        return self.duration_bgl_ms + self.duration_ext_ms + self.duration_bgt_ms + self.duration_exl_ms

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
    def binsize_ms(self):
        """Analyses binsizes in ms

        Returns
        -------
        binsize: np.ndarray of int
            Size of base analysis time bins in ms
        """
        return self._binsize_ms

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
    def duration_bgl_ms(self):
        """Leading background duration in ms

        Returns
        -------
        duration : int
            Duration of leading background window in ms
        """
        return self._duration_bgl_ms

    @property
    def duration_bgt_ms(self):
        """Trailing background duration in ms

        Returns
        -------
        duration : int
            Duration of trailing background window in ms
        """
        return self._duration_bgt_ms

    @property
    def duration_exl_ms(self):
        """Leading exclusion duration in ms

        Returns
        -------
        duration : int
            Duration of leading exclusion window in ms
        """
        return self._duration_exl_ms

    @property
    def duration_ext_ms(self):
        """Trailing exclusion duration in ms

        Returns
        -------
        duration : int
            Duration of trailing exclusion window in ms
        """
        return self._duration_ext_ms

    @property
    def dur_trigger_window(self):
        """trigger window duration in ms
        """
        return self._dur_trigger_window

    @property
    def dur_signi_buffer(self):
        """xi buffer duration in ms
        """
        return self._dur_signi_buffer

    @property
    def trigger_condition(self):
        """Primary Trigger condition, the threshold at which a trigger escalates into a candidate
        """
        return self._trigger_condition


class AnalysisHandler:
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

    def __init__(self, config, ndom=5160, eps=None, dtype=np.uint16,
                 start_time=np.datetime64('now'), dropped_doms=None):
        """Create Analysis Handler

        Parameters
        ----------
        config : AnalysisConfig
            Instance of Analysis configuration object
        ndom : int
            number of contributing DOMs
        eps : numpy.ndarray
            relative efficiency of contributing DOMs
        dtype
            Data type for SN scaler arrays
        start_time : np.datetime64
            Timestamp of the first scaler received by analysis
        dropped_doms : None or np.ndarray
            np.ndarray[ndom] containing indices of the DOMs to be dropped from analysis upon startup

        """
        self.config = config

        # Create shared window buffer
        self._binnings = config.binsize_ms
        self._ndom = ndom
        if eps is None:
            self._eps = np.where(np.arange(5160) > 4800, np.ones(5160), 1.35 * np.ones(5160))
        else:
            self._eps = eps
        self._dtype = dtype
        self._start_time = start_time
        self._start_utime = datetime64_to_utime(start_time)

        if dropped_doms is not None:
            self._eps = np.delete(self._eps, dropped_doms, axis=0)
            self._ndom -= dropped_doms.size
            # TODO: check for compatibility between self._ndom and ndom argument when dropped doms are present

        # Size is computed so the following may be included in buffer
        #   Leading/trailing background and exclusion windows, and search window (duration_nosearch + max(binnings))
        #   Max analysis offset (max(binnings) - base_binsize)
        #   Rates to subtract from buffer during analysis (max(binnings))
        self._size = ((config.duration_nosearch + 3 * self.config.max_binsize) // config.base_binsize) - 1
        self._rebin_factor = int(config.base_binsize / config.raw_binsize)
        self.buffer_raw = windowbuffer(size=self._size * self._rebin_factor, ndom=ndom, dtype=dtype)
        self.buffer_analysis = windowbuffer(size=self._size, ndom=self._ndom, dtype=np.uint64)
        self.buffer_xi = windowbuffer(size=config.dur_signi_buffer, ndom=len(self.config.binsize_ms), dtype=np.float64)

        # Create analyses
        self.analyses = []
        n = 0
        for binning in np.asarray(self._binnings, dtype=dtype):
            for offset in np.arange(0, binning, 500, dtype=dtype):  # TODO: Increment by binsize not 500
                idx = int(self._size - (config.duration_nosearch + offset + binning) / config.base_binsize)
                self.analyses.append(
                    Analysis(config, binning, offset, idx=idx, ndom=self._ndom, start_time=self._start_time, n_ana=n)
                )
                n += 1

        Analysis.base_binsize_ms = self.config.base_binsize

        # Define counter for accumulation used in rebinning from raw to base analysis
        self._accum_count = self._rebin_factor
        self._accum_data = np.zeros(ndom, dtype=dtype)

        self.candidates = []
        self.trigger_count = 0
        self.cand_count = 0
        self.trigger_xi = 0.
        self.triggered_analysis = None
        self._n_bins_trigger_window = int(config.dur_trigger_window / config.base_binsize)
        self._n_trigger_close = int(self.config.trigger_condition.dt_to_close / config.base_binsize)
        logger.debug('Analysis Handler Initialized.')

    def set_start_time(self, start_time):
        """Sets time where the leading edge of the raw buffer sits

        Parameters
        ----------
        start_time : np.datetime64
            Analysis start time (Not run start time, necessarily)
        """
        self._start_time = start_time
        self._start_utime = datetime64_to_utime(start_time)
        year = start_time.astype('datetime64[Y]').item().year
        for ana in self.analyses:
            ana.start_time = start_time
            ana.year = year
            ana.utime_sw = datetime64_to_utime(start_time) - ((ana.idx_eod - ana.idx_sw) * int(ana._base_binsize * 1e7))

    def status(self):
        """Obtain a status string

        Returns
        -------
        status_string :str
        """
        status = ''.join([
            f"Processing {len(self.analyses):>2d} Analyses"
        ])
        return status

    def get_lightcurve(self, ana, dur_lct, dur_lcl):
        """Export hits from all DOMs (Lightcurve) from data buffer

        Parameters
        ----------
        ana : sndaq.analysis.Analysis
            SNDAQ Analysis object, contains relevant indices and meta data
        dur_lct : int
            Duration of trailing lightcurve in ms (Time after t0=0)
        dur_lcl : int
            Duration of leading lightcurve in ms (Time before t0=0)

        Returns
        -------
        lightcurve : np.ndarray of int
            Binned SN hits in requested binning, spanning [t0-dur_lct, t0+dur_lcl]
            Note: The first bin may be a partial bin, depending on the modulus between the total lightcurve duration
            and the requested binning.
        """
        # Pre-allocate
        mod_lct = dur_lct % ana.binsize
        nbins_lct = int(dur_lct // ana.binsize + int(bool(mod_lct)))
        mod_lcl = dur_lct % ana.binsize
        nbins_lcl = int(dur_lcl // ana.binsize + int(bool(mod_lcl)))
        lightcurve = np.zeros(shape=(nbins_lct + nbins_lcl, self.ndom))

        nbins_rawt = int(dur_lct // self.config.base_binsize)  # Guaranteed to have no modulus
        nbins_rawl = int(dur_lcl // self.config.base_binsize)

        rebin_factor = int(ana.binsize // self.config.base_binsize)
        nbins_shift = int(mod_lcl / self.config.base_binsize )
        # TODO: Add to error checking that lc duration is multiple of min binsize
        idx = (np.arange(nbins_rawt+nbins_rawl)+nbins_shift) // rebin_factor

        # Performs rebinning automatically - ana.binsize is integer multiple of analysishandler.config.base_binsize
        # Reshapes are requires for np.add.at signature - 2nd arg, index needs shape ((n, 1), (1, m)) for (m, n) target
        np.add.at(lightcurve, (idx.reshape(-1, 1), np.arange(self.ndom).reshape(1, -1)),
                  self.buffer_analysis[ana.idx_sw - nbins_rawl:ana.idx_sw + nbins_rawt, :])

        return lightcurve

    def get_avg_lightcurve(self, ana, dur_lct, dur_lcl):
        """Export Average hits per DOM (Avg. Lightcurve) from data buffer

        Parameters
        ----------
        ana : sndaq.analysis.Analysis
            SNDAQ Analysis object, contains relevant indices and meta data
        dur_lct : int
            Duration of trailing lightcurve in ms (Time after t0=0)
        dur_lcl : int
            Duration of leading lightcurve in ms (Time before t0=0)

        Returns
        -------
        lightcurve : np.ndarray of int
            Average SN hit rate in requested binning
        """
        return self.get_lightcurve(ana, dur_lct, dur_lcl).mean(axis=1)

    @property
    def trigger_pending(self):
        """Boolean indicating whether a trigger candidate is currently pending. SNDAQ will check for higher triggers for
        up to `_n_bins_trigger_window` bins worth of time (default 30s)
        """
        return self.buffer_analysis.n <= self._n_trigger_close

    def open_trigger_window(self):
        """Open a 30 s time window for a pending trigger. After 30s worth of data is added to buffer, the window closes.

        See Also: trigger_pending

        Note: An "open" trigger window refers to the state where SNDAQ will overwrite the current trigger with another
        of higher significance, a "closed" window refer to the state where the current trigger may not be overwritten.
        Calling this function before a window closes will extend it for 30 s.
        """
        # TODO: Change the name or construction of this, n implies that it is a number of bins relative to some point
        self._n_trigger_close = self.buffer_analysis.n + self._n_bins_trigger_window

    @property
    def ndom(self):
        """Number of DOMs contributing to Analyses
        """
        return self._ndom

    @property
    def current_time(self):
        """Timestamp of data entering the analysis buffer
        """
        return self._start_time + np.timedelta64(int(self.buffer_analysis.n * self.config.base_binsize / 1e3), 's')

    @property
    def current_utime(self):
        """UTC Timestamp of data entering the analysis buffer
        """
        return datetime64_to_utime(self.current_time)

    def trigger_time(self, ana=None):
        """

        Parameters
        ----------
        ana : sndaq.analysis.Analysis

        Returns
        -------

        """
        if ana is None:
            ana = self.analyses[-1]

        if ana.is_online:
            return self.current_time - np.timedelta64(int(ana.n_eod_sw * ana.base_binsize_ms / 1e3), 's')
        else:
            # This represents time 0 in the Unix Epoch: 1970-01-01 00:00:00 - intended as a sort of minimum value
            return np.datetime64(0, 'Y')

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
            print(f'{i:d} {analysis.binsize * 1e-3:4.1f} (+{analysis.offset * 1e-3:4.1f})')

    def update_analyses(self):
        """Update SICO sums and computed quantities for all analyses
        """
        for analysis in self.analyses:
            analysis.utime_sw += int(self.config.base_binsize * 1e7)
            self.update_sums(analysis)

            if analysis.is_updatable:
                if analysis.is_online:
                    # Perform validation before computing analysis quantities
                    self.validate_analysis(analysis)
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

            analysis.hit_sum2 += (add_to_bgl ** 2 + add_to_bgt ** 2)
            analysis.hit_sum2 -= (sub_from_bgl ** 2 + sub_from_bgt ** 2)

    def update_results(self, analysis):
        """Update SICO analysis results

        Parameters
        ----------
        analysis : sndaq.analysis.Analysis
            Analysis object for which to update quantities computed from sums
        """
        # Analysis results are updated by the handler as the analysis class (currently) is intended to be a container
        #   The handler is intended to contain the algorithms
        idx = analysis.dom_status
        mean = analysis.mean[idx]
        var = analysis.var[idx]
        rate = analysis.rate[idx]
        eps = self.eps[idx]
        signal = rate - mean

        sum_rate_dev = np.divide(signal * eps, var, out=np.zeros_like(signal), where=var > 0).sum()
        sum_inv_var = np.divide(eps**2,  var, out=np.zeros_like(eps), where=var > 0).sum()
        analysis.dmu = sum_rate_dev / sum_inv_var
        analysis.var_dmu = 1. / sum_inv_var

        # calc xi
        analysis.xi = analysis.dmu / np.sqrt(analysis.var_dmu)

        # calc chi2
        # tmp = (signal*(1. - eps))**2 / (var + eps*abs(signal))
        _num = (rate - (mean + eps * signal)) ** 2
        _denom = (var + eps * abs(signal))
        analysis.chi2 = np.divide(_num, _denom, out=np.zeros_like(_num), where=_denom > 0).sum()

    def accumulate(self, val, idx):
        """Accumulate 2 ms data into analysis binsize

        Parameters
        ----------
        val : numpy.ndarray of int
            ndom-length array of 2 ms scaler hits to be accumulated into higher order binnings.
        idx : numpy.ndarray of int
            Indices of DOMs at which to add 2 ms scaler hits

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

    def _validate_bounded_quantity(self, ana, quantity, q_min, q_max, name=None):
        """ Perform validation on analysis quantity based on config-specified bounds.
        DOMs failing validation (quantities outside bounds) are excluded from analysis sums
        Default Quantites are bkg Hit rate: mean, variance, & fano

        TODO: Would making this a member of the analysis class be better for performance, so as to prevent multiple
            instances of the same analysis object?

        Parameters
        ----------
        ana : sndaq.analysis.Analysis
            Analysis currently begin validated
        quantity : np.ndarray[float]
            Analysis quantity on which to perform validation
        q_min : float
            Lower bound of `quantity`
        q_max : float
            Upper bound of `quantity`
        name : str
            Name of quantity used for logs

        See Also
        --------
        etc/analysis.ini

        Returns
        -------
        mask_good : np.ndarray[bool]
            Boolean mask of currently-invalid DOMs that should be included in Analysis
        mask_bad : np.ndarray[bool]
            Boolean mask of currently-valid DOMs that should be excluded from Analysis
        """
        # Condition indicates that quant falls within bounds (True = "Good")
        cond = (q_min < quantity) & (quantity < q_max)

        # Detect changes, if no changes take no action
        if not np.all(cond == ana.dom_status):
            # Find good doms that have failed validation, and exclude them from analysis
            #   (dom_status == True [Good DOM] and cond_bkg == False [DOM failed validation])
            mask_bad = ana.dom_status & ~cond

            # Find bad doms that have passed validation, and include them analysis
            #   (dom_status == False [Bad DOM] and cond_bkg == True [DOM passed validation])
            mask_good = ~ana.dom_status & cond

            return mask_good, mask_bad
        else:
            # True in this case indicates tha this DOM's status should be change, by default make no changes
            return np.zeros_like(cond), np.zeros_like(cond)

    def validate_analysis(self, analysis):
        """Performs validation on an SNDAQ Analysis. Checks are performed DOM-by-DOM and
        offending DOMs' contributions are removed from analysis quantities.
        """
        # Validation only occurs on analyses that are ready to report results (is_online)
        #    and are ready to be updated (is_updatable), meaning a new sum can be computed
        if analysis.is_online and analysis.is_updatable:

            # Analysis has enough contributing DOMs [bool]
            cond_ndom = analysis.ndom > self.config.min_active_doms
            if (analysis.is_valid and not cond_ndom) or (~analysis.is_valid and cond_ndom):
                analysis.is_valid = ~analysis.is_valid
                logger.debug(f"Analysis #{analysis.n_ana} nDOM check changed state!  is_valid: {analysis.is_valid}")

            # DOM-wise checks
            mask_good_mean, mask_bad_mean = self._validate_bounded_quantity(analysis, analysis.mean,
                                                                           self.config.min_bkg_rate,
                                                                           self.config.max_bkg_rate)
            mask_good_fano, mask_bad_fano = self._validate_bounded_quantity(analysis, analysis.fano,
                                                                           self.config.min_bkg_fano,
                                                                           self.config.max_bkg_fano)
            mask_good = mask_good_mean & mask_good_fano
            mask_bad = mask_bad_mean & mask_bad_fano

            if np.any(mask_bad):
                logger.debug(f"Analysis #{analysis.n_ana}: {mask_bad.sum()} DOMs removed after failing validation")
            if np.any(mask_good):
                logger.debug(f"Analysis #{analysis.n_ana}: {mask_good.sum()} DOMs added after passing validation")
            # TODO: Add Jitter & Noise Validation
            # TODO: Add monitoring quantity for number of state changes
            # TODO: Check if analysis sums present rates in Hz or counts (/binsize)

    def update(self, value):
        # TODO: Figure out how to enable streaming only analysis-binning data
        """Update raw buffer, analysis buffer, analyses sums and analysis results

        Parameters
        ----------
        value : numpy.ndarray
            2ms data for each DOM at a particular timestamp
        """
        self.buffer_raw.append(value)
        idx = value.nonzero()[0]  # Returns tuple, the first element of which is indices of non-zero elem
        if not self.accumulate(value[idx], idx):
            # Accumulator indicates time to reset, as base analysis bin of data is ready
            # TODO: Find a more intuitive way of doing this.
            accumulated_data = np.asarray(self._accum_data, dtype=np.uint16)
            self.reset_accumulator()
            self.buffer_analysis.append(accumulated_data)
            self.update_analyses()
            # Get triggerable analyses [ana for ana in self.analyses if ana.is_online and ana.is_triggerable]
            # For only those analyses, evaluate if a trigger threshold has been met
            # For FRA, could also check if analysis search window also overlaps with trigger time
            self.process_triggers()

    def process_triggers(self):
        """Check if any analysis meets the primary trigger threshold.
        """
        # TODO: Check performance of this function
        # TODO: Move this into the trigger handler if possible

        # Get all potentially triggered analyses
        potential_analyses = [(i, ana) for i, ana in enumerate(self.analyses)
                              if self.config.trigger_condition.check(ana)]

        # Decide what to do with them, Escalating triggers must be handled differently from Fast Response Triggers
        if potential_analyses:
            # Detect Escalating trigger
            if self.config.trigger_condition is PrimaryTrigger:
                xi = np.array([ana.xi for (_, ana) in potential_analyses])
                xi_max = xi.max(initial=0.0)

                if xi_max > self.candidates[0].xi:
                    self.trigger_count += 1
                    # Extend trigger window after new highest trigger
                    self.open_trigger_window()

                    # This is a little obtuse, but idx here refers to the index of ana in self.analyses
                    idx = potential_analyses[xi.argmax()][0]
                    ana = self.analyses[idx]

                    # Corrected signi is set upon candidate becoming finalized, performed by trigger handler
                    # TODO: Set this up with from_analysis method
                    self.candidates[0] = Trigger.from_analysis(ana, self.trigger_count, self.cand_count + 1)

            if self.config.trigger_condition is FastResponseTrigger:
                self.candidates += [Trigger.from_analysis(ana, 1, self.cand_count+1+idx)
                                    for (idx, ana) in potential_analyses]
                logger.info(f"New FR Triggers formed in analysis {potential_analyses}")
                # TODO: Streamline this when you move it to the trigger handler
                self._n_trigger_close = 0  # NOTE: This is a hack to immediately finalize the trigger
            # TODO: Fix counting for cands

    def get_buffered_xi(self, binsize):
        """Return buffered xi in requested binsize

        Parameters
        ----------
        binsize : int
            Analysis binsize in units ms. Must match one of the binnings provided at initialization

        Returns
        -------
        data : np.ndarray of float
            Buffered xi in the requested binsize
        """
        idx_bin = self.config.binsize_ms.argsort(binsize)[0][0]

        # Guard against 0-padding at start of run
        if self.buffer_xi.n < (self.config.dur_signi_buffer // self.config.base_binsize):
            # Do not return non-populated entries in buffer
            return self.buffer_xi.data[-self.buffer_xi.n:, idx_bin]
        # TODO: Add check for end of run
        return self.buffer_xi.data[:, idx_bin]

    def get_buffered_rmu(self, binsize):
        """Return buffered muon rates in requested binsize

        Parameters
        ----------
        binsize : int
            Analysis binsize in units ms. Must match one of the binnings provided at initialization

        Returns
        -------
        data : np.ndarray of float
            muon rates in the requested binsize
        """
        return NotImplemented

    def prepare_candidate(self, rmu, rmu_500):
        """Prepares SN candidate for muon correction and further processing
        """
        self.candidates[0].buffer_xi = self.get_buffered_xi(self.candidates[0].binsize)
        self.candidates[0].buffer_rmu = {'trigger_binsize': rmu,
                                         '500ms': rmu_500}

    @property
    def trigger_finalized(self):
        """Indicator for whether the currently pending trigger is ready for processing
            If True, the trigger is ready to be processed, the trigger window (`open_trigger_window()`) is closed
            If False, the trigger window has not yet closed, more data must be processed
        """
        return len(self.candidates) > 0 and not self.trigger_pending


class Analysis:
    """Descriptor object to handle data access and algorithms for SNDAQ sico-analysis
    """
    base_binsize_ms = 500

    def __init__(self, config, binsize, offset, idx=0, ndom=5160, start_time=0):
        """Create Analysis object

        Parameters
        ----------
        config : AnalysisConfig
            Instance of Analysis configuration object
        binsize : int
            Time size of bins in signal rate and background rate calculation
        offset : int
            Time offset of analysis window in ms
        idx : int
            Starting index in analysis buffer, includes time offset
        ndom : int
            Number of DOMs contributing to the analysis
        start_time : np.datetime64
            UTC time at the start of Analysis
        """
        if (binsize % config.base_binsize) > 0:  # Binsize must be an integer multiple of base_binsize
            raise RuntimeError(f'Binsize {binsize:d} ms is incompatible, must be factor of {config.base_binsize:d} ms')
        self._binsize = binsize  # ms
        self._base_binsize = config.base_binsize  # ms
        self._offset = offset  # ms
        self._rebin_factor = int(self._binsize / config.base_binsize)
        # TODO: Decide if ndom should always be 5160 or the number of doms in the current config

        # Bookkeeping quantities
        self._ndom = ndom
        self._dom_status = np.ones(self._ndom, dtype=bool)
        self.n_ana = n_ana
        self.is_valid = True

        self._nbin_nosearch = config.duration_nosearch / self._binsize
        self._nbin_background = (config.duration_bgl_ms + config.duration_bgt_ms) / self._binsize

        # Indices for accessing data buffer, all point to first column in respective region
        # TODO: Check alignment so all start filling as soon as possible
        self._idx_bgt = idx  # Trailing background
        self._idx_ext = self._idx_bgt + int(config.duration_bgt_ms / self._base_binsize)  # Trailing exclusion
        self._idx_sw = self._idx_ext + int(config.duration_ext_ms / self._base_binsize)  # Search window
        self._idx_exl = self._idx_sw + int(self.binsize / self._base_binsize)  # Leading exclusion
        self._idx_bgl = self._idx_exl + int(config.duration_exl_ms / self._base_binsize)  # Leading background
        self.idx_eod = self._idx_bgl + int(config.duration_bgl_ms / self._base_binsize)  # End of data in analysis
        self._n_eod_sw = self.idx_eod - self.idx_sw

        # Indices of Analysis buffer for "bins" to add to sums for analysis
        # Using np.arange here (np arrays as indices) allows all analyses to be indexed in the same way
        # It's important to compute this only once, as these indices will never change for a given analysis
        self._idx_addbgl = np.arange(self.idx_eod - self.rebin_factor, self.idx_eod)  # Add to leading bg
        self._idx_subbgl = np.arange(self.idx_bgl - self.rebin_factor, self.idx_bgl)  # Subtract from leading bg
        self._idx_addbgt = np.arange(self.idx_ext - self.rebin_factor, self.idx_ext)  # Add to trailing bg
        self._idx_subbgt = np.arange(self.idx_bgt - self.rebin_factor, self.idx_bgt)  # Subtract from trailing bg
        self._idx_addsw = np.arange(self.idx_exl - self.rebin_factor, self.idx_exl)  # Add to search window
        self._idx_subsw = np.arange(self.idx_sw - self.rebin_factor, self.idx_sw)  # Subtract from search window

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
        self.n_to_trigger = self.idx_eod - self.idx_bgt + int(self.offset / config.base_binsize)
        self.n = 0
        self.start_time = start_time
        self.year = start_time.astype('datetime64[Y]').item().year
        self.utime_sw = datetime64_to_utime(start_time) - ((self.idx_eod - self.idx_sw) * int(self._base_binsize * 1e7))
        logger.debug(f"Analysis {self.binsize}+({self.offset}) Initialized")

    def __repr__(self):
        repr_str = f"SNDAQ Binned Search #{self.n_ana:<2d}: {self.binsize} +({self.offset}) s"
        return repr_str


    def status(self):
        """Obtain a status string

        Returns
        -------
        status_string :str
        """
        status = '\n'.join([
            repr(self),
            f"is_triggerable: {self.is_triggerable}, is_updatable: {self.is_updatable}, is_online: {self.is_online}",
            f"n={self.n}, n_accum={self.n_accum}, n_to_trigger={self.n_to_trigger}",
            f"xi={self.xi}, dmu={self.dmu}, sig_dmu={self.var_dmu}, hit_sum={self.hit_sum}",
        ])
        return status

    @property
    def trigger_utime(self):
        """Time (measured in ns since year start) that is represented by this analysis' search window
        """
        return self.utime_sw if self.is_online else -np.inf

    @property
    def trigger_datetime64(self):
        """Time in ms that is represented by this analysis' search window
        """
        if self.is_online:
            return utime_to_datetime64(self.utime_sw, year=self.year)
        else:
            return np.datetime64("NaT")

    def reset_accum(self):
        """Reset Analysis accumulator sums after collecting 500 ms of data
        """
        self.n_accum = 0

    def remove_doms(self, idc):
        """Remove DOMs from analysis specified by indices in sum arrays
        TODO: Change indices to DOM ID?

        Parameters
        ----------
        idc : Indices in sum array of DOM to remove from analysis
        """
        self._ndom -= idc.size
        self._dom_status[idc] = False

    def add_doms(self, idc):
        """Add DOMs to analysis specified by indices in sum arrays. Intended for use on re-validated DOMs
        TODO: Change indices to DOM ID?

        Parameters
        ----------
        idc : Indices in sum array of DOM to add back into analysis
        """
        self._ndom += idc.size
        self._dom_status[idc] = True

    @property
    def dom_status(self):
        """Array of Bool indicating which DOMs contribute (true) or are excluded (false) from this analysis
        """
        return self._dom_status

    @property
    def ndom(self):
        """Number of DOMs currently contributing to this Analysis
        """
        return self._ndom

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
        mean : np.ndarray[float]
            Mean of background hit rate per bin measured across both background windows
        """
        # TODO: Unit test for float type!
        return self.hit_sum / self.nbin_bg

    @property
    def var(self):
        """Background rate variance

        Returns
        -------
        variance : np.ndarray[float]
            Variance of background hit rate per bin measured across both background windows
        """
        # TODO: Unit test for float type!
        return ((self.nbin_bg * self.hit_sum2) - (self.hit_sum ** 2)) / self.nbin_bg ** 2

    @property
    def fano(self):
        """Background rate Fano factor (variance-to-mean ratio)

        Returns
        -------
        fano : np.ndarray[float]
            DOM-wise fano factor for all DOMs
        """
        # TODO: Unit test for float type!
        return np.divide(self.var, self.mean, out=np.zeros_like(self.var), where=self.mean != 0)

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
        return (self._nbin_nosearch + 1) * self.binsize

    @property
    def n_eod_sw(self):
        return self._n_eod_sw

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
