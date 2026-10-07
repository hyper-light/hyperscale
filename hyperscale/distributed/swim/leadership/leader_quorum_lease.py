"""
A leader's quorum lease: how long this node may still hold leadership.
"""


class LeaderQuorumLease:
    """
    The lease a leader holds only while a majority of its cohort keeps
    acknowledging it (Raft thesis 6.2 CheckQuorum, 6.4.1 leases).

    A follower renews its own lease, which refuses pre-votes and keeps it
    from standing, on every heartbeat it applies -- at receipt, so no
    earlier than the beat was sent -- and holds it for the leader's
    granted duration. A beat a follower acknowledges therefore keeps it
    from helping elect anyone else until the beat's SEND instant plus that
    duration. Once a majority (this leader included) acknowledged beats
    sent at or after some instant, no other node can win a majority before
    that instant plus the duration, so the leader holds leadership exactly
    that long and must step down when it ends -- two leaders can never
    hold the flag at one instant.

    The term opens at the leader's claim: every vote that won it was cast
    after the claim was sent, and a voter refuses pre-votes for a lease
    duration after voting, so the claim's send instant anchors the lease
    until the first beats are acknowledged.

    Memory: beats older than one lease duration can never extend the lease
    and are dropped as new ones are sent; acknowledgements are one entry
    per cohort voter and cleared each term.
    """

    def __init__(self) -> None:
        self._term: int = -1
        self._claim_sent_at: float = 0.0
        self._beat_sent_at_by_term_and_sequence: dict[tuple[int, int], float] = {}
        self._acknowledged_sent_at_by_voter: dict[tuple[str, int], float] = {}

    def open_term(self, term: int, claim_sent_at: float) -> None:
        """Start the lease for ``term``, anchored at the claim's send instant."""
        self._term = term
        self._claim_sent_at = claim_sent_at
        self._beat_sent_at_by_term_and_sequence.clear()
        self._acknowledged_sent_at_by_voter.clear()

    def record_beat(self, term: int, sequence: int, sent_at: float, lease_duration: float) -> None:
        """Remember when beat ``sequence`` of ``term`` was sent; forget beats too old to matter."""
        oldest_useful_sent_at = sent_at - lease_duration
        self._beat_sent_at_by_term_and_sequence = {
            beat_key: beat_sent_at
            for beat_key, beat_sent_at in self._beat_sent_at_by_term_and_sequence.items()
            if beat_sent_at >= oldest_useful_sent_at
        }
        self._beat_sent_at_by_term_and_sequence[(term, sequence)] = sent_at

    def record_acknowledgement(self, voter: tuple[str, int], term: int, sequence: int) -> None:
        """Credit ``voter`` with the send instant of the beat it acknowledged, when newer.

        Only beats this leader sent in this lease's term are known, so an
        acknowledgement of any other term's beat credits nothing.
        """
        sent_at = self._beat_sent_at_by_term_and_sequence.get((term, sequence), float("-inf"))
        if term == self._term and sent_at > self._acknowledged_sent_at_by_voter.get(voter, self._claim_sent_at):
            self._acknowledged_sent_at_by_voter[voter] = sent_at

    def expires_at(self, term: int, peers_needed: int, lease_duration: float) -> float:
        """The instant ``term``'s lease ends, given the peer acknowledgements a majority needs.

        The ``peers_needed``-th most recent acknowledged send instant, or the
        claim's while fewer peers have acknowledged a beat (every recorded
        acknowledgement is newer than the claim). A node that is its own
        majority holds it indefinitely; a term never claimed holds none.
        """
        if peers_needed <= 0:
            return float("inf")
        if term != self._term:
            return float("-inf")
        newest_first = sorted(self._acknowledged_sent_at_by_voter.values(), reverse=True)
        newest_first.extend([self._claim_sent_at] * peers_needed)
        return newest_first[peers_needed - 1] + lease_duration
