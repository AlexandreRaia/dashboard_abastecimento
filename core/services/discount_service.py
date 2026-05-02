"""
Aplicação de desconto financeiro sobre os registros de abastecimento.

O desconto representa o abatimento contratual negociado com os fornecedores
de combustível e deve ser aplicado antes de qualquer KPI financeiro.
"""
from __future__ import annotations

import pandas as pd


def apply_discount(df: pd.DataFrame, discount_rate: float) -> pd.DataFrame:
    """
    Aplica o percentual de desconto à coluna 'valor'.

    O DataFrame resultante contém:
    - 'valor'        : valor já com desconto aplicado (usado em KPIs)
    - 'valor_bruto'  : valor original antes do desconto (para auditoria)
    - 'desconto_valor': valor absoluto do desconto (para exibição)

    Args:
        df:            DataFrame com pelo menos a coluna 'valor'.
        discount_rate: Taxa de desconto entre 0.0 e 1.0 (ex.: 0.04 = 4%).

    Returns:
        Novo DataFrame com as três colunas financeiras populadas.

    Raises:
        ValueError: Se discount_rate estiver fora do intervalo [0, 1].
    """
    if not (0.0 <= discount_rate <= 1.0):
        raise ValueError(
            f"discount_rate deve estar entre 0.0 e 1.0; recebido: {discount_rate}"
        )

    result = df.copy()

    if "valor" not in result.columns:
        return result

    result["valor_bruto"] = result["valor"].copy()
    result["desconto_valor"] = result["valor"] * discount_rate
    result["valor"] = result["valor"] * (1.0 - discount_rate)

    return result
