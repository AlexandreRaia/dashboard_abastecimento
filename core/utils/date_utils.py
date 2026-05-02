"""
Utilitários de parsing e manipulação de datas.

Fonte única da verdade para conversão de datas — elimina a duplicação
entre Abastecimento.py e pages/Auditoria.py.
"""
from __future__ import annotations

import datetime
import pandas as pd


def parse_date_series(series: pd.Series) -> pd.Series:
    """
    Converte uma Series de datas brutas para datetime.

    Trata:
    - Strings ISO (YYYY-MM-DD, YYYY-MM-DD HH:MM:SS)
    - Strings DD/MM/YYYY
    - Seriais numéricos do Excel (dias desde 1899-12-30)
    - Timestamps já convertidos

    Args:
        series: Series com valores de data em qualquer formato suportado.

    Returns:
        Series de pd.Timestamp (NaT onde não foi possível converter).
    """
    dates = pd.to_datetime(series, dayfirst=True, errors="coerce")

    # Fallback: seriais numéricos do Excel (ex.: 45292 → 2023-12-31)
    numeric_vals = pd.to_numeric(series, errors="coerce")
    excel_mask = dates.isna() & numeric_vals.notna()
    if excel_mask.any():
        dates.loc[excel_mask] = pd.to_datetime(
            numeric_vals[excel_mask], unit="D", origin="1899-12-30", errors="coerce"
        )

    return dates


def date_range_years(
    data_inicio: datetime.date | None,
    data_fim: datetime.date | None,
) -> list[int]:
    """
    Retorna a lista de anos abrangidos pelo intervalo de datas.

    Se nenhuma data for fornecida, retorna [ano corrente].
    """
    if data_inicio is None and data_fim is None:
        return [datetime.date.today().year]
    start = data_inicio or datetime.date(2020, 1, 1)
    end = data_fim or datetime.date.today()
    return sorted(set(range(start.year, end.year + 1)))
