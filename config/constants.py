"""
Constantes globais da aplicação — sem lógica, sem I/O.

Centraliza todos os mapeamentos, paletas e dicionários de lookup
para garantir consistência entre o dashboard e o pipeline de auditoria.
"""

# ---------------------------------------------------------------------------
# Calendário
# ---------------------------------------------------------------------------
MONTHS: dict[int, str] = {
    1: "Janeiro",
    2: "Fevereiro",
    3: "Março",
    4: "Abril",
    5: "Maio",
    6: "Junho",
    7: "Julho",
    8: "Agosto",
    9: "Setembro",
    10: "Outubro",
    11: "Novembro",
    12: "Dezembro",
}

MONTHS_SHORT: dict[int, str] = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr",
    5: "Mai", 6: "Jun", 7: "Jul", 8: "Ago",
    9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
}

MONTH_NAME_TO_NUMBER: dict[str, int] = {v: k for k, v in MONTHS.items()}

# ---------------------------------------------------------------------------
# Combustíveis — normalização de nomes brutos para internos
# ---------------------------------------------------------------------------
FUEL_MAP: dict[str, str] = {
    # Gasolina
    "GASOLINA":         "GASOLINA",
    "GASOLINA COMUM":   "GASOLINA",
    "GASOLINA ADITIVADA": "GASOLINA",
    # Álcool / Etanol
    "ALCOOL":           "ALCOOL",
    "ÁLCOOL":           "ALCOOL",   # com acento
    "ETANOL":           "ALCOOL",
    "ETANOL HIDRATADO": "ALCOOL",
    "ALCOOL HIDRATADO": "ALCOOL",
    "ÁLCOOL HIDRATADO": "ALCOOL",
    # Diesel
    "DIESEL":           "DIESEL S10",
    "DIESEL COMUM":     "DIESEL S10",
    "DIESEL S10":       "DIESEL S10",
    "DIESEL S-10":      "DIESEL S10",
    "OLEO DIESEL":      "DIESEL S10",
    "ÓLEO DIESEL":      "DIESEL S10",
}

# ---------------------------------------------------------------------------
# Paleta de cores por combustível (Plotly hex)
# Fonte única da verdade — elimina divergências entre gráficos
# ---------------------------------------------------------------------------
FUEL_COLORS: dict[str, str] = {
    "GASOLINA":   "#2563eb",   # azul
    "ALCOOL":     "#22c55e",   # verde
    "ÁLCOOL":     "#22c55e",   # verde (com acento — fallback)
    "ETANOL":     "#22c55e",   # verde (alias)
    "DIESEL S10": "#f97316",   # laranja
}

# ---------------------------------------------------------------------------
# Paleta de anos para gráficos comparativos (YoY)
# Adicione novos anos conforme necessário
# ---------------------------------------------------------------------------
YEAR_PALETTE: dict[int, str] = {
    2022: "#bfdbfe",   # azul muito claro
    2023: "#7dd3fc",   # azul claro
    2024: "#38bdf8",   # azul médio-claro
    2025: "#60a5fa",   # azul médio
    2026: "#1d4ed8",   # azul escuro (ano mais recente)
}
