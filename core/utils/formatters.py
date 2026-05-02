"""
Utilitários de formatação de valores.

Fonte única da verdade para formatação de moeda, litros e percentual.
Importar daqui em vez de redefinir `currency()` em cada módulo.
"""
from __future__ import annotations


def currency(value: float | int) -> str:
    """
    Formata um valor numérico como moeda brasileira (BRL).

    Exemplos:
        currency(1234.56)  → 'R$ 1.234,56'
        currency(0)        → 'R$ 0,00'
    """
    if value is None:
        return "R$ 0,00"
    return f"R$ {float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def format_litros(value: float | int, decimals: int = 0) -> str:
    """
    Formata volume em litros com separador de milhar brasileiro.

    Exemplos:
        format_litros(12345.6)   → '12.346 L'
        format_litros(12345.6, 2) → '12.345,60 L'
    """
    fmt = f"{float(value):,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{fmt} L"


def format_percent(value: float, decimals: int = 1, sign: bool = False) -> str:
    """
    Formata percentual com separador decimal brasileiro.

    Args:
        value:    Valor percentual (ex.: 12.5 → '12,5%').
        decimals: Casas decimais.
        sign:     Se True, prefixo '+' quando positivo.
    """
    prefix = "+" if sign and value > 0 else ""
    fmt = f"{value:.{decimals}f}".replace(".", ",")
    return f"{prefix}{fmt}%"


def format_km(value: float | int) -> str:
    """Formata quilometragem com separador de milhar."""
    return f"{float(value):,.0f} km".replace(",", ".")
