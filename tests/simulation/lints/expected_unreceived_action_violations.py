"""Snapshot for test_every_sent_action_has_a_receiver: every entry is a
sent TCP action no node receives, awaiting its fix. The ratchet only
shrinks."""

EXPECTED_UNRECEIVED_ACTION_VIOLATIONS: frozenset[str] = frozenset()
