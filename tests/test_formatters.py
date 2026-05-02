"""Testes unitários para core/utils/formatters.py."""
import pytest
from core.utils.formatters import currency, format_litros, format_percent, format_km


class TestCurrency:
    def test_zero(self):
        assert currency(0) == "R$ 0,00"

    def test_positive_integer(self):
        result = currency(1234)
        assert result == "R$ 1.234,00"

    def test_positive_float(self):
        result = currency(1234.56)
        assert result == "R$ 1.234,56"

    def test_large_value(self):
        result = currency(1_000_000)
        assert "1.000.000" in result

    def test_negative(self):
        # Valores negativos são formatados com sinal
        result = currency(-500.0)
        assert "-" in result or "500" in result

    def test_none_returns_zero(self):
        # None é tratado como 0
        result = currency(None)
        assert result == "R$ 0,00"


class TestFormatLitros:
    def test_basic(self):
        result = format_litros(12346)
        assert "12.346" in result
        assert "L" in result

    def test_zero(self):
        result = format_litros(0)
        assert "0" in result

    def test_decimals(self):
        result = format_litros(100.5, decimals=1)
        assert "100" in result


class TestFormatPercent:
    def test_basic(self):
        result = format_percent(12.5)
        assert "12,5" in result
        assert "%" in result

    def test_with_sign_positive(self):
        result = format_percent(5.0, sign=True)
        assert "+" in result

    def test_with_sign_negative(self):
        result = format_percent(-5.0, sign=True)
        assert "-" in result

    def test_zero(self):
        result = format_percent(0.0)
        assert "0" in result


class TestFormatKm:
    def test_basic(self):
        result = format_km(1234)
        assert "1.234" in result
        assert "km" in result
