"""Testes unitários para core/services/discount_service.py."""
import pytest
import pandas as pd
from core.services.discount_service import apply_discount


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "valor": [100.0, 200.0, 0.0],
        "placa": ["AAA-0001", "BBB-0002", "CCC-0003"],
    })


class TestApplyDiscount:
    def test_zero_discount(self, sample_df):
        result = apply_discount(sample_df, 0.0)
        assert (result["valor"] == sample_df["valor"]).all()

    def test_discount_adds_columns(self, sample_df):
        result = apply_discount(sample_df, 0.04)
        assert "valor_bruto" in result.columns
        assert "desconto_valor" in result.columns

    def test_discount_value_correct(self, sample_df):
        result = apply_discount(sample_df, 0.04)
        # valor_bruto = valor original (antes do desconto)
        # valor = valor_bruto * (1 - 0.04)
        assert abs(result.iloc[0]["valor_bruto"] - 100.0) < 0.01
        assert abs(result.iloc[0]["valor"] - 96.0) < 0.01

    def test_invalid_rate_above_one(self, sample_df):
        with pytest.raises(ValueError):
            apply_discount(sample_df, 1.5)

    def test_invalid_rate_below_zero(self, sample_df):
        with pytest.raises(ValueError):
            apply_discount(sample_df, -0.1)

    def test_empty_dataframe(self):
        result = apply_discount(pd.DataFrame(), 0.04)
        assert result.empty

    def test_discount_of_zero_row(self, sample_df):
        result = apply_discount(sample_df, 0.04)
        # Row com valor=0 — bruto deve ser 0 também
        assert result.iloc[2]["valor_bruto"] == 0.0
