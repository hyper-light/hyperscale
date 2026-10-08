"""
Log entry schema versions across a group whose members run different
builds (AD-52 section 14) -- real ``RaftNode`` instances, every message
delivered by hand, on virtual time.

* A leader writes the newest schema every member reads, as each last
  reported it -- and, until every member has reported, the oldest it
  writes: a member still on the older build could apply nothing newer.
* A member handed an entry it cannot read never skips it: applying stops
  there -- every entry after it too, however readable -- and says where. A
  skipped entry would fork its state silently from every other member's.
"""

from tests.simulation.harness.sim import VirtualClock

from .test_raft_snapshot_install import Group, _simulate

NEWER_BUILD = (1, 2)
OLDER_BUILD = (1, 1)


def test_the_leader_writes_the_newest_schema_every_member_reads() -> None:
    async def scenario(clock: VirtualClock) -> tuple[int, int, int]:
        mixed = Group(["A", "B", "C"], clock, {"A": NEWER_BUILD, "B": NEWER_BUILD, "C": OLDER_BUILD})
        before_reports = mixed.node("A").write_schema_version
        await mixed.elect("A", mixed.among("A", "B", "C"))
        mixed_version = mixed.node("A").write_schema_version

        upgraded = Group(["D", "E", "F"], clock, {"D": NEWER_BUILD, "E": NEWER_BUILD, "F": NEWER_BUILD})
        await upgraded.elect("D", upgraded.among("D", "E", "F"))
        return before_reports, mixed_version, upgraded.node("D").write_schema_version

    assert _simulate(scenario) == (1, 1, 2)


def test_a_member_never_applies_an_entry_it_cannot_read() -> None:
    async def scenario(clock: VirtualClock) -> tuple[list[str], list[str], int | None, int]:
        group = Group(["A", "B", "C"], clock, {"A": NEWER_BUILD, "B": NEWER_BUILD, "C": OLDER_BUILD})
        everyone = group.among("A", "B", "C")
        await group.elect("A", everyone)
        await group.append_as_leader("A", ["readable-1"], schema_version=1)
        # An entry of the newer schema reaches C (as if C joined after the
        # group moved on), then one it could read again.
        await group.append_as_leader("A", ["newer"], schema_version=2)
        await group.append_as_leader("A", ["readable-2"], schema_version=1)
        await group.replicate("A", everyone)
        await group.replicate("A", everyone)
        for member_id in ("B", "C"):
            await group.node(member_id).apply_committed_entries()
        newer_index = group.node("A").last_log_index - 1
        return (
            group.members["B"].applied,
            group.members["C"].applied,
            group.node("C").apply_halted_at,
            newer_index,
        )

    b_applied, c_applied, c_halted_at, newer_index = _simulate(scenario)

    assert b_applied == ["readable-1", "newer", "readable-2"]
    assert c_applied == ["readable-1"]
    assert c_halted_at == newer_index
