"""Objects for handling SNDAQ Triggers
"""
import numpy as np
from abc import ABC, abstractmethod


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


class PrimaryTrigger(_TriggerLevel):
    """Primary Trigger
    """
    name = 'primary'
    threshold = 4.0
    threshold_corr = 4.0

    @classmethod
    def process(cls, trigger):
        print('Primary Trigger Processing...')
        print(trigger)


# Experimenting with escalating trigger scheme. The idea is you would init the highest matching trigger class, and the
#    processing would propagate down to lower level triggers, executing from high -> low threshold
class BasicTrigger(_TriggerLevel):
    """Basic Trigger
    """
    name = 'basic'
    threshold = 6.0
    threshold_corr = 4.0

    @classmethod
    def process(cls, trigger):
        print('Basic Trigger Processing...')
        PrimaryTrigger.process(trigger)


class SNWGTrigger(_TriggerLevel):
    """Supernova Working Group (Internal) Trigger
    """
    name = 'sn-wg'
    threshold = 7.0
    threshold_corr = 4.4

    @classmethod
    def process(cls, trigger):
        print('SN-WG Processing...')
        BasicTrigger.process(trigger)


class SNEWSTrigger(_TriggerLevel):
    """SNEWS (External) Trigger
    """
    name = 'snews'
    threshold = 8.4
    threshold_corr = 5.8

    @classmethod
    def process(cls, trigger):
        print('SNEWS Processing...')
        SNWGTrigger.process(trigger)


class SilverTrigger(_TriggerLevel):
    """'Silver' Trigger
    """
    name = 'silver'
    threshold = 8.0
    threshold_corr = 8.0

    @classmethod
    def process(cls, trigger):
        print('Silver Processing...')
        SNEWSTrigger.process(trigger)


class GoldTrigger(_TriggerLevel):
    """'Gold' Trigger
    """
    name = 'gold'
    threshold = 10.0
    threshold_corr = 10.0

    @classmethod
    def process(cls, trigger):
        print('Gold Processing...')
        SilverTrigger.process(trigger)


class TriggerConfig:
    """Configuration object for SICO analysis trigger handling
    """

    # TODO: Add/connect reader for SNDAQ Config

    # In SNDAQ, Each threshold is defined differently.
    # Also, "___TriggerLevel" refers to muon-corrected triggers. For now this scheme is reversed in PySNDAQ,
    # As I (sgriswold) find this more intuitive, and am starting with testing against triggers w/o muon correction
    _trigger_levels = sorted((
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
    def __init__(self):
        super().__init__()

    def process_trigger(self, trigger):
        """Process incoming SN trigger

        Parameters
        ----------
        trigger : sndaq.triggers.Trigger
            SNDAQ Trigger Object

        Returns
        -------

        """
        trigger_level = next((level for level in self._trigger_levels if (trigger.xi >= level.threshold or
                                                                          trigger.xi_corr >= level.threshold_corr)),
                             None)
        trigger_level.process(trigger)


class Trigger:
    """Container object for SNDAQ Trigger Candidate
    """
    def __init__(self, xi=0, xi_corr=0, t=0, binsize=None, offset=None):
        # Parameters
        # ----------
        # threshold : np.ndarray
        #     Uncorrected xi threshold for issuing a SN trigger alert
        # corr_threshold : float
        #     Corrected xi threshold for issuing a SN trigger alert

        """PySNDAQ Trigger

        Parameters
        ----------
        xi : float
            SNDAQ (uncorrected) TS
        xi_corr : float
            SNDAQ muon-corrected TS
        t : float
            Time of trigger (Time of bin at trailing edge of AnalysisHandler.buffer_analysis search window)
        binsize : float
            Size of analysis search window in ms from which the trigger was issued
        offset : float
            Offset of search window in ms from which the trigger was issued
        """
        self.xi = xi
        self.xi_corr = xi_corr
        self.t = t
        self.binsize = binsize
        self.offset = offset

    def __repr__(self):
        return f"xi={self.xi:5.3f} in {self.binsize / 1e3:>5.1f} +({self.offset / 1e3}) s Analysis @t={self.t} s"

    def reset(self):
        self.xi = 0
        self.xi_corr = 0
        self.t = 0
        self.binsize = None
        self.offset = None

    # @classmethod
    # def from_analysis(cls, ana, time):
    #     """Create a trigger object from a sndaq.analysis.Analysis object
    #
    #     Parameters
    #     ----------
    #     ana : sndaq.analysis.Analysis
    #
    #     Returns
    #     -------
    #     Trigger : sndaq.trigger.Trigger
    #
    #     """
    #     return cls.__init__(ana.xi, 0, ana.time, ana.binsize,  )

