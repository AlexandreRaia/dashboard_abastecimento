"""
Filtragem de registros de abastecimento.

Encapsula os filtros de período, secretaria e combustível para garantir
consistência entre o dashboard e qualquer outro consumidor dos dados.
"""
from __future__ import annotations

import datetime
import pandas as pd


def apply_filters(
    df: pd.DataFrame,
    data_inicio: datetime.date | None,
    data_fim: datetime.date | None,
    selected_secretaria: str | list,
    selected_combustivel: str | list,
) -> pd.DataFrame:
    """
    Filtra o DataFrame de abastecimentos pelos critérios fornecidos.

    Args:
        df:                   DataFrame com pelo menos 'data_hora', 'secretaria'
                              e 'combustivel'.
        data_inicio:          Data de início do período (inclusivo), ou None.
        data_fim:             Data de fim do período (inclusivo), ou None.
        selected_secretaria:  Sigla(s) da secretaria — string ('Todas') ou lista.
        selected_combustivel: Nome(s) do combustível — string ('Todos') ou lista.

    Returns:
        DataFrame filtrado (cópia, não modifica o original).
    """
    filtered = df.copy()

    if data_inicio is not None and "data_hora" in filtered.columns:
        filtered = filtered[filtered["data_hora"].dt.date >= data_inicio]

    if data_fim is not None and "data_hora" in filtered.columns:
        filtered = filtered[filtered["data_hora"].dt.date <= data_fim]

    # Normaliza para lista — aceita string, lista ou None
    def _to_list(val):
        if val is None:
            return []
        if isinstance(val, str):
            return [] if val in ("Todas", "Todos", "") else [val]
        return list(val)

    sec_list = _to_list(selected_secretaria)
    if sec_list and "secretaria" in filtered.columns:
        sec_upper = [s.upper() for s in sec_list]
        filtered = filtered[filtered["secretaria"].str.upper().isin(sec_upper)]

    comb_list = _to_list(selected_combustivel)
    if comb_list and "combustivel" in filtered.columns:
        comb_upper = [c.upper() for c in comb_list]
        filtered = filtered[filtered["combustivel"].str.upper().isin(comb_upper)]

    return filtered
