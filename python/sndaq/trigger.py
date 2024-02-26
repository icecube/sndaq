"""Objects for handling SNDAQ Triggers
"""
import numpy as np
from abc import ABC, abstractmethod
from enum import Enum
from sndaq.logger import get_logger

logger = get_logger()


class TriggerBase(ABC):
    """Trigger condition base class
    """
    name: str

    @classmethod
    @abstractmethod
    def check(cls, *args, **kwargs) -> bool:
        """Check if an arbitrary trigger condition has been met

        Returns
        -------
        trigger_met : bool
            True if trigger condition has been met, False if not
        """
        pass


class PrimaryTrigger(TriggerBase):
    """Primary trigger condition for Online SNDAQ
    """
    name = "primary"
    threshold = 4.0
    threshold_corr = 0.0

    @classmethod
    def check(cls, ana):
        """Check if an SNDAQ Analysis object meets the primary trigger

        Parameters
        ----------
        ana : sndaq.analysis.Analysis
            SNDAQ Analysis Object

        Returns
        -------
        trigger_met : bool
            True if trigger condition has been met, False if not
        """
        return ana.is_triggerable and ana.xi > 4


class FastResponseTrigger(TriggerBase):
    """Fast Response Trigger Condition
    """
    name = "fast_response"
    trigger_time = None

    @classmethod
    def set_trigger_time(cls, trigger_time):
        """Set Fast Response Trigger time. It is implicitly assumed that only one trigger time is defined at a time

        Parameters
        ----------
        trigger_time : np.datetime64
            Fast Response Trigger time
        """
        cls.trigger_time = trigger_time

    @classmethod
    def check(cls, ana):
        """Check if an SNDAQ Analysis object meets the Fast Response Trigger condition

        Parameters
        ----------
        ana : sndaq.analysis.Analysis

        Returns
        -------
        trigger_met : bool
            True if Analysis' search window overlaps with
        """
        if not ana.is_triggerable:
            return False
        # Broken into two parts because the following processing isn't necessary if the analysis is untriggerable
        ana_time = ana.trigger_datetime64
        trigger_time_matches = (ana_time < cls.trigger_time &
                                cls.trigger_time < ana_time + np.timedelta64(ana.binsize, 'ms'))
        return trigger_time_matches

class EscalationTrigger(TriggerBase):
    """Base class for Escalating xi threshold triggers
    """
    name: str
    threshold: float
    threshold_corr: float

    @classmethod
    def check(cls, cand):
        """Check if SNDAQ Candidate object meets the escalating trigger condition

        Parameters
        ----------
        cand : sndaq.trigger.Candidate

        Returns
        -------
        trigger_met : bool
            True if either candidate's xi or xi_corr exceed this trigger's threshold or threshold_corr, respectively
        """
        return cand.xi > cls.threshold or cand.xi_corr > cls.threshold_corr

    @classmethod
    def process_cand(cls, cand):
        """Process SN Candidate
        """
        logger.info(f'{cls.name}-level processing...')
        logger.info(' - Would perform muon correction')

    @classmethod
    def process_alert(cls, alert):
        """Process SN Candidate (intended for Northern Hemisphere use)
        """
        raise NotImplementedError


class BasicTrigger(EscalationTrigger):
    """Basic Trigger level for SNDAQ Escalation Scheme
    """
    name = "basic"
    threshold = 6.0
    threshold_corr = 4.0


class SNWGTrigger(EscalationTrigger):
    """SN-WG Trigger level for SNDAQ Escalation Scheme
    """
    name = "sn-wg"
    threshold = 7.0
    threshold_corr = 4.4


class SNEWSTrigger(EscalationTrigger):
    """SNEWS Trigger level for SNDAQ Escalation Scheme
    """
    name = "snews"
    threshold = 8.4
    threshold_corr = 5.8


class SilverTrigger(EscalationTrigger):
    """Silver Trigger level for SNDAQ Escalation Scheme
    """
    name = "silver"
    threshold = 8.0
    threshold_corr = 8.0


class GoldTrigger(EscalationTrigger):
    """Gold Trigger level for SNDAQ Escalation Scheme
    """
    name = "gold"
    threshold = 10.0
    threshold_corr = 10.0


class _TriggerLevel(ABC):
    """Base Class for PySNDAQ Trigger levels, can be used to define additional trigger levels
    """

    name = None
    threshold = np.inf  # Uncorrected
    threshold_corr = np.inf  # Corrected

    @classmethod
    @abstractmethod
    def process(cls, trigger):
        """Process PySNDAQ Supernova Trigger
        """
        pass

    def __hash__(self):
        return hash((self.name, self.threshold, self.threshold_corr))

    def __eq__(self, other):
        return self.name == other.name and \
               self.threshold == other.threshold and \
               self.threshold_corr == other.threshold_corr


class TriggerConfig:
    """Configuration object for SICO analysis trigger handling
    """

    # TODO: Add/connect reader for SNDAQ Config

    # In SNDAQ, Each threshold is defined differently.
    # Also, "___TriggerLevel" refers to muon-corrected triggers. For now this scheme is reversed in PySNDAQ,
    # As I (sgriswold) find this more intuitive, and am starting with testing against triggers w/o muon correction
    _trigger_levels = sorted((
        PrimaryTrigger,
        BasicTrigger,
        SNWGTrigger,
        SNEWSTrigger,
        SilverTrigger,
        GoldTrigger,
    ), key=lambda level: level().threshold, reverse=False)

    trigger_levels_dict = {level: (level.threshold, level.threshold_corr) for level in _trigger_levels}

    def __init__(self, levels=None, levels_corr=None):
        pass
        # if levels and levels_corr:
        #     self.trigger_levels = {key: (levels[key], levels_corr[key]) for key in levels}
        # TODO: Make sure this doesn't modify the trigger_levels member of other instanced trigger handlers
        # Such a scenario (multiple alert handlers in one scope would be atypical, but best to be sure.

    # Listing potential other members, which may be useful for trigger reporting
    # pdaq_run
    # sndaq_run
    # lid_flag


class TriggerHandler(TriggerConfig):
    """Handler class for SICO Analysis Triggers
    """
    def __init__(self, primary_trigger=PrimaryTrigger):
        self.primary_trigger = primary_trigger
        super().__init__()

    def process_cand(self, cand):
        """Process incoming SN candidate

        Parameters
        ----------
        cand : sndaq.triggers.Trigger
            SNDAQ Candidate Object

        Returns
        -------

        """
        if isinstance(self.primary_trigger, FastResponseTrigger):
            pass
        elif isinstance(self.primary_trigger, PrimaryTrigger):
            pass

        levels_to_process = [level for level in self._trigger_levels if (cand.xi >= level.threshold or
                                                                         cand.xi_corr >= level.threshold_corr)]
        # Process from highest level to lowest
        for level in reversed(levels_to_process):
            level.process(cand)


class Trigger:
    """Container object for SNDAQ Trigger Candidate
    """
    def __init__(self, ana=None, xi=0, xi_corr=0, t=0, binsize=0, offset=0, trigger_no=0, cand_no=0):
        # Parameters
        # ----------
        # threshold : np.ndarray
        #     Uncorrected xi threshold for issuing a SN trigger alert
        # corr_threshold : float
        #     Corrected xi threshold for issuing a SN trigger alert

        """PySNDAQ Trigger

        Parameters
        ----------
        ana : sndaq.analysis.Analysis
            Analysis in which trigger formed
        xi : float
            SNDAQ (uncorrected) TS
        xi_corr : float
            SNDAQ muon-corrected TS
        t : np.datetime64
            Time of trigger (Time of bin at trailing edge of AnalysisHandler.buffer_analysis search window)
        binsize : int
            Size of analysis search window in ms from which the trigger was issued
        offset : int
            Offset of search window in ms from which the trigger was issued
        trigger_no : int
            Number of trigger for the current candidate
        cand_no : int
            Number of candidate for the current run
        """
        self.ana = ana
        self.xi = xi
        self.xi_corr = xi_corr
        self.t = t
        self.binsize = binsize
        self.offset = offset
        self.trigger_no = trigger_no
        self.cand_no = cand_no

    def __repr__(self):
        return f"Candidate {self.cand_no:>3d}, Trigger {self.trigger_no:>3d}: "  \
               f"xi={self.xi:5.3f} in {self.binsize / 1e3:>5.1f} +({self.offset / 1e3}) s Analysis @t={self.t} s"

    def reset(self):
        self.xi = 0
        self.xi_corr = 0
        self.t = 0
        self.binsize = None
        self.offset = None

    @classmethod
    def from_analysis(cls, ana, trigger_no, cand_no):
        """Create a trigger object from a sndaq.analysis.Analysis object

        Parameters
        ----------
        ana : sndaq.analysis.Analysis
        trigger_no : int
            Number of trigger for the current candidate
        cand_no : int
            Number of candidate for the current run

        Returns
        -------
        Trigger : sndaq.trigger.Trigger

        """
        return cls.__init__(xi=ana.xi, xi_corr=0, t=ana.trigger_utime, binsize=ana.binsize,
                            offset=ana.offset, trigger_no=trigger_no, cand_no=cand_no)


class Candidate:
    """SN Event Candidate
    """
    def __init__(self, ana):
        """

        Parameters
        ----------
        ana : sndaq.analysis.Analysis
        """
        self.ana = ana
        self.xi = None
        self.xi_corr = None
        self.rmu_trigger = None
        self.rmu_base = None
        self.rmu_files = None



