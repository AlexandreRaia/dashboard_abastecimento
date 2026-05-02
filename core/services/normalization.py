"""
Normalização de campos de texto (secretaria, combustível, grupo).

Fonte única da verdade — elimina divergências de casing e abreviaturas
entre o dashboard e o pipeline de auditoria.
"""
from __future__ import annotations

from config.constants import FUEL_MAP

# ---------------------------------------------------------------------------
# Aliases de secretaria: variantes encontradas na fonte → sigla canônica
# ---------------------------------------------------------------------------
_SECRETARIA_ALIASES: dict[str, str] = {
    "SEMUTTRANS": "SMTT",
    "SEMUTRANS":  "SMTT",
}


def normalize_secretaria(value: str | None) -> str:
    """
    Normaliza o nome de secretaria para a sigla canônica.

    Args:
        value: Nome bruto da secretaria (pode ser None).

    Returns:
        Sigla em maiúsculas, sem espaços extras.
    """
    normalized = str(value or "").strip().upper()
    return _SECRETARIA_ALIASES.get(normalized, normalized)


def normalize_fuel(value: str | None) -> str:
    """
    Mapeia o nome de combustível da fonte para a categoria interna.

    Exemplos:
        'ETANOL' → 'ALCOOL'
        'DIESEL S10' → 'DIESEL S10'
        'gasolina' → 'GASOLINA'

    Args:
        value: Nome bruto do produto/combustível.

    Returns:
        Nome normalizado em maiúsculas.
    """
    raw = str(value or "").strip().upper()
    return FUEL_MAP.get(raw, raw)


def classify_fuel_group(value: str | None) -> str:
    """
    Classifica um combustível em um dos três grupos para análise.

    Grupos:  GASOLINA | DIESEL | ALCOOL

    Diesel S10 é agrupado com DIESEL.
    Etanol é agrupado com ALCOOL.
    Qualquer outro valor é retornado em maiúsculas sem agrupamento.

    Args:
        value: Nome interno do combustível (já normalizado ou não).

    Returns:
        Grupo de combustível em maiúsculas.
    """
    raw = str(value or "").strip().upper()
    if raw.startswith("DIESEL"):
        return "DIESEL S10"
    if raw in {"ALCOOL", "ETANOL"}:
        return "ALCOOL"
    return raw
