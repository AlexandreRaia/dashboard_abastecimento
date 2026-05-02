"""
Pacote de configuração do sistema KPIs Abastecimento.

Exporta o objeto `settings` (singleton) e as constantes globais.
"""
from .settings import settings
from .constants import (
    MONTHS,
    MONTHS_SHORT,
    MONTH_NAME_TO_NUMBER,
    FUEL_MAP,
    FUEL_COLORS,
    YEAR_PALETTE,
)

__all__ = [
    "settings",
    "MONTHS",
    "MONTHS_SHORT",
    "MONTH_NAME_TO_NUMBER",
    "FUEL_MAP",
    "FUEL_COLORS",
    "YEAR_PALETTE",
]
