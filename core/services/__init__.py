"""Pacote de serviços de domínio."""
from .normalization import normalize_secretaria, normalize_fuel, classify_fuel_group
from .discount_service import apply_discount
from .filter_service import apply_filters
from .kpi_service import (
    build_kpis,
    build_secretaria_status,
    build_monthly_mix,
    build_alerts,
    build_ranking,
    month_count,
)

__all__ = [
    "normalize_secretaria",
    "normalize_fuel",
    "classify_fuel_group",
    "apply_discount",
    "apply_filters",
    "build_kpis",
    "build_secretaria_status",
    "build_monthly_mix",
    "build_alerts",
    "build_ranking",
    "month_count",
]
