class RunSummaryStopped(Exception):
    """A run's CI-safe summary stopped writing progress on an error of its
    own (not a failed write, which it reports instead)."""
