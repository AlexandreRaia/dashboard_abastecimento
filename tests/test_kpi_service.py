"""Testes unitários para core/services/kpi_service.py."""
import pytest
import pandas as pd
from core.services.kpi_service import build_kpis, build_secretaria_status, month_count


@pytest.fixture
def sample_df():
    """DataFrame mínimo de abastecimentos para testes."""
    return pd.DataFrame({
        "data":        pd.to_datetime(["2025-01-15", "2025-02-20", "2025-02-25"]),
        "ano":         [2025, 2025, 2025],
        "mes":         [1, 2, 2],
        "mes_nome":    ["Janeiro", "Fevereiro", "Fevereiro"],
        "valor":       [1000.0, 2000.0, 500.0],
        "litros":      [100.0, 180.0, 45.0],
        "secretaria":  ["SMCC", "SMDS", "SMCC"],
        "combustivel": ["GASOLINA", "DIESEL", "GASOLINA"],
        "placa":       ["AAA-0001", "BBB-0002", "AAA-0001"],
        "km_por_litro": [10.0, 8.0, 10.0],
        "custo_por_km": [1.0, 1.5, 1.0],
    })


@pytest.fixture
def sample_limits():
    return pd.DataFrame({
        "secretaria":       ["SMCC", "SMDS"],
        "ano":              [2025, 2025],
        "valor_empenhado":  [30000.0, 50000.0],
        "limite_mensal":    [2500.0, 4167.0],
    })


class TestMonthCount:
    def test_two_months(self, sample_df):
        # month_count uses 'ano_mes' column; ensure it's present
        df = sample_df.copy()
        df["ano_mes"] = df["ano"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2)
        assert month_count(df) == 2

    def test_empty(self):
        # month_count returns 1 for empty DF (avoids division by zero)
        assert month_count(pd.DataFrame()) == 1

    def test_single_month(self):
        df = pd.DataFrame({"ano": [2025, 2025], "mes": [1, 1]})
        assert month_count(df) == 1


class TestBuildKpis:
    def test_returns_dict(self, sample_df, sample_limits):
        status = build_secretaria_status(sample_df, sample_limits)
        kpis = build_kpis(sample_df, status, sample_limits)
        assert isinstance(kpis, dict)

    def test_gasto_total(self, sample_df, sample_limits):
        status = build_secretaria_status(sample_df, sample_limits)
        kpis = build_kpis(sample_df, status, sample_limits)
        assert abs(kpis["gasto_total"] - 3500.0) < 0.01

    def test_required_keys(self, sample_df, sample_limits):
        status = build_secretaria_status(sample_df, sample_limits)
        kpis = build_kpis(sample_df, status, sample_limits)
        required = {"gasto_total", "n_abastecimentos", "veiculos_ativos", "gasto_litros"}
        assert required.issubset(kpis.keys())

    def test_empty_df(self):
        # build_kpis with empty df should not crash and return 0 for gasto_total
        empty = pd.DataFrame(columns=["valor", "litros", "placa", "km_por_litro", "custo_por_km", "secretaria", "ano", "mes", "ano_mes"])
        kpis = build_kpis(empty, pd.DataFrame(), pd.DataFrame())
        assert kpis["gasto_total"] == 0.0


class TestBuildSecretariaStatus:
    def test_returns_dataframe(self, sample_df, sample_limits):
        result = build_secretaria_status(sample_df, sample_limits)
        assert isinstance(result, pd.DataFrame)

    def test_has_secretaria_column(self, sample_df, sample_limits):
        result = build_secretaria_status(sample_df, sample_limits)
        assert "secretaria" in result.columns

    def test_empty_df(self):
        result = build_secretaria_status(pd.DataFrame(), pd.DataFrame())
        assert result.empty or isinstance(result, pd.DataFrame)
