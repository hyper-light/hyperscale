import msgspec


class GroupCreatedRecord(msgspec.Struct, frozen=True, tag="group_created", array_like=True):
    """A group's first record: the member this node took part in it as,
    and the voters it was created with -- the same on every member, and in
    no log entry -- so a member resuming it starts from the configuration
    every other member did, and only as the member it was."""

    group_id: str
    member_id: str
    initial_voters: list[str]
