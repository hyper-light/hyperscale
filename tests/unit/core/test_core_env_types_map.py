"""
The core jobs Env's types_map covers exactly its fields.

``types_map`` names the parser each setting's environment variable is
read with; a field without an entry is never read from the environment,
and an entry without a field names a setting that does not exist (as
MERCURY_SYNC_MAX_WORKFLOWS did, while MERCURY_SYNC_AUTH_SECRET_PREVIOUS
and five other fields had no entry).
"""

from hyperscale.core.jobs.models.env import Env


def test_every_field_has_a_parser():
    assert sorted(set(Env.model_fields) - set(Env.types_map())) == []


def test_every_parser_names_a_field():
    assert sorted(set(Env.types_map()) - set(Env.model_fields)) == []
