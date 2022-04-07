class CitationData:
    """Citation data class containing extracted data."""

    def __init__(self, citing, cited, created, timespan, journal_sc, author_sc):
        """Citation data constructor.

        Args:
            citing (str): the id of the citing resource
            cited (str): the id of the cited resource
            created (str): creation time
            timespan (str): timespan
            journal_sc (bool, optional): true if it is a journal self-cited
            author_sc (bool, optional): true if it is a author self-cited
        """
        self.citing = citing
        self.cited = cited
        self.created = created
        self.timespan = timespan
        self.timespan = timespan
        self.journal_sc = journal_sc
        self.author_sc = author_sc
