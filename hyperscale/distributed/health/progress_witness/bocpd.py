"""
Bayesian Online Change-Point Detection (Adams & MacKay, 2007).

Detects when the underlying distribution of an observation stream
shifts — without a magic threshold. The only inputs are a hazard
prior (parameter-free, uniform over run lengths) and the predictive
likelihood family (Student-t induced by a Normal-inverse-Gamma
conjugate prior over Gaussian observations).

Used by the AD-26 Phase H6 throughput witness to flag regime shifts
in per-(worker, workflow) throughput. A drop in throughput appears
as a change-point with the new run's posterior mean below the prior
run's posterior mean — the witness consults the *direction* of the
shift to distinguish "throughput regressed → potential stuck
workflow" from "throughput surged → no concern."

Reference:
    Adams, R. P., & MacKay, D. J. C. (2007). Bayesian Online
    Change-Point Detection. arXiv:0710.3742.

Notation in the implementation matches the paper:
    r_t            run length at time t
    P(r_t | x_1:t) run-length posterior
    H(r)           hazard function — P(change-point | run length r)
    pi_t^(r)       predictive probability under run length r

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Sequence

from .bayesian_online_change_point_detector import _student_t_log_pdf
from .bocpd_shared import _predictive_params_from_suffstats
from .bayesian_online_change_point_detector import _logsumexp
from .bocpd_config import BOCPDConfig
from .bayesian_online_change_point_detector import BayesianOnlineChangePointDetector
from .run_length_posterior import RunLengthPosterior
from ._run_suff_stats import _RunSuffStats

_REHOMED = (
    BOCPDConfig,
    _RunSuffStats,
    RunLengthPosterior,
    BayesianOnlineChangePointDetector,
    _student_t_log_pdf,
    _predictive_params_from_suffstats,
    _logsumexp,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
