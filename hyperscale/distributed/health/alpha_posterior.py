"""Per-workflow-class Beta-conjugate posterior on extension success.

AD-26 Phase H8 outcome feedback loop: every time a workflow
terminates, its outcome (completed vs. timed-out / failed /
evicted) is fed into a Bayesian tuner keyed on the workflow's
Python class name. The posterior is a standard Beta(α, β) over
the success probability ``p`` of the workflow class. Its failure
evidence re-weights the H6 hierarchical α of every throughput-
witness test on a workflow of that class
(``HierarchicalAlphaTuner.alpha_budget``): classes that fail more
often get more of the false-positive budget, classes that rarely
fail get less (Genovese, Roeder & Wasserman 2006 p-value weighting).

Why Beta:

* Conjugate to Bernoulli: closed-form sufficient stats means the
  update is O(1) and lossless (no sample-size truncation).
* Bounded: the failure rate stays in (0, 1) without clamping, so
  the weight it yields is finite and positive.
* Persists cleanly: the posterior collapses to two floats — easy
  to ship in ``TimeoutTrackingState`` for AD-34 leader-transfer
  survivability.

Why per-class (not per-workflow-id):

* Each workflow_id is one trajectory; we'd never have enough data
  to learn anything per-id. Per-class learning aggregates across
  every load-test instance of the same Python class, so the tuner
  becomes useful within minutes on a moderately-busy cluster.
* AD-26 explicitly calls for the tuner to be "robust, performant,
  resource efficient" — the keyspace is bounded by the user's
  workflow-class count, typically O(10) to O(100) on a real
  cluster.

Initialization:

* New classes store the ``alpha_prior``/``beta_prior`` offset (see
  ``alpha_posterior_shared``); the α budget reads only the evidence
  net of it, shrunk toward the failure rate pooled over all classes.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import Iterator
from hyperscale.distributed.health.extension_outcome import ExtensionOutcomeEvent, ExtensionOutcomeKind

from .alpha_posterior_shared import _DEFAULT_ALPHA_PRIOR
from .alpha_posterior_shared import _DEFAULT_BETA_PRIOR
from .workflow_class_alpha_posterior import _POSTERIOR_FIELD_COUNT
from .hierarchical_alpha_tuner import HierarchicalAlphaTuner
from .hierarchical_alpha_tuner_config import HierarchicalAlphaTunerConfig
from .workflow_class_alpha_posterior import WorkflowClassAlphaPosterior

_REHOMED = (
    WorkflowClassAlphaPosterior,
    HierarchicalAlphaTunerConfig,
    HierarchicalAlphaTuner,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
