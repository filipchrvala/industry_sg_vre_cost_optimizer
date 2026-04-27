"""
Základná trieda pre všetky pieces (Domino workflow).

V Domino runtime sa zvyčajne nastaví ``results_path`` na výstupný priečinok kroku
pred volaním ``piece_function``. Lokálne používame ``Piece.__new__`` + priradenie cesty.
"""


class BasePiece:
    """Minimálny spoločný predok – rozšíriteľný v plnom Domino SDK."""

    results_path: str | None = None

    def piece_function(self, input_data):  # type: ignore[no-untyped-def]
        raise NotImplementedError
