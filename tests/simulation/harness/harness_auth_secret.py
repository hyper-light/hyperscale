"""The cluster secret every node and client a harness cluster builds shares.

There is no default cluster secret (the encryptor refuses a missing, short
or known-weak one), so the harness names one explicitly.
"""

HARNESS_AUTH_SECRET = "hyperscale-simulation-harness-secret-0123456789"
