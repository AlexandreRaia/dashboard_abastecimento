"""Testes unitários para core/services/normalization.py."""
import pytest
from core.services.normalization import normalize_secretaria, normalize_fuel, classify_fuel_group


class TestNormalizeSecretaria:
    def test_alias_semuttrans(self):
        assert normalize_secretaria("SEMUTTRANS") == "SMTT"

    def test_alias_semutrans(self):
        assert normalize_secretaria("SEMUTRANS") == "SMTT"

    def test_unknown_passthrough(self):
        assert normalize_secretaria("SMCC") == "SMCC"

    def test_strips_whitespace(self):
        assert normalize_secretaria("  SMCC  ") == "SMCC"

    def test_lowercase_input(self):
        assert normalize_secretaria("smcc") == "SMCC"

    def test_none_input(self):
        result = normalize_secretaria(None)
        assert isinstance(result, str)


class TestNormalizeFuel:
    def test_gasolina(self):
        assert normalize_fuel("gasolina") == "GASOLINA"

    def test_etanol_to_alcool(self):
        assert normalize_fuel("ETANOL") == "ALCOOL"

    def test_alcool(self):
        assert normalize_fuel("ALCOOL") == "ALCOOL"

    def test_diesel_s10(self):
        assert normalize_fuel("DIESEL S10") == "DIESEL S10"

    def test_unknown_passthrough(self):
        assert normalize_fuel("GNV") == "GNV"


class TestClassifyFuelGroup:
    def test_gasolina(self):
        assert classify_fuel_group("GASOLINA") == "GASOLINA"

    def test_alcool(self):
        assert classify_fuel_group("ALCOOL") == "ALCOOL"

    def test_etanol_maps_to_alcool(self):
        assert classify_fuel_group("ETANOL") == "ALCOOL"

    def test_diesel(self):
        assert classify_fuel_group("DIESEL") == "DIESEL S10"

    def test_diesel_s10_maps_to_diesel(self):
        assert classify_fuel_group("DIESEL S10") == "DIESEL S10"

    def test_unknown(self):
        result = classify_fuel_group("GNV")
        assert isinstance(result, str)
