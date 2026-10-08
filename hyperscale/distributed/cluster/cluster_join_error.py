class ClusterJoinError(Exception):
    """A requested cluster join was refused or could not complete.

    Raised with an operator-readable reason; the join coordinator turns
    it into a rejected ``NodeJoinResponse`` instead of letting it reach
    the transport layer.
    """
