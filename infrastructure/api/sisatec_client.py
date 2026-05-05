"""
Cliente HTTP para a API Sisatec de abastecimento.

Encapsula toda a lógica de paginação, conversão de campos e
tratamento de erros, mantendo a UI livre de código de integração.
"""
from __future__ import annotations

import datetime
import json
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from config.settings import settings


@dataclass
class SisatecRecord:
    """Representa um único registro de abastecimento retornado pela API."""

    data_hora: str | None
    placa: str
    condutor: str
    marca: str
    modelo: str
    ano_veiculo: str
    ult_km: float | None
    km_atual: float | None
    km_l: float | None
    km_rodado: float | None
    litros: float | None
    valor_litro: float | None
    valor: float | None
    produto: str
    unidade: str
    estabelecimento: str
    registro: str
    prefixo: str
    tipo_frota: str
    custo_por_km: float | None


def _br_float(value: Any) -> float | None:
    """Converte valor numérico brasileiro (vírgula decimal) para float."""
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "."))
    except (ValueError, TypeError):
        return None


def _parse_date_iso(date_str: str, time_str: str) -> str | None:
    """Converte data DD/MM/YYYY + hora HH:MM para ISO 'YYYY-MM-DD HH:MM'."""
    if not date_str:
        return None
    try:
        date_iso = datetime.datetime.strptime(date_str.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
        return f"{date_iso} {time_str.strip()}".strip()
    except ValueError:
        return date_str


def _record_from_dict(raw: dict) -> dict:
    """
    Converte um dict bruto da API para o schema interno (colunas do banco).

    As chaves produzidas correspondem às colunas esperadas pela tabela
    'abastecimentos' no SQLite e pelo load_abastecimentos().
    """
    km_atual = _br_float(raw.get("kmAtual"))
    km_ant = _br_float(raw.get("kmAnterior"))
    km_rod_api = _br_float(raw.get("KmHoraRodado"))
    km_rod = km_rod_api if (km_rod_api and km_rod_api > 0) else (
        (km_atual - km_ant) if (km_atual and km_ant and km_atual > km_ant) else None
    )
    litros = _br_float(raw.get("quantidadeLitros"))
    kml_api = _br_float(raw.get("KmHoraPorLitro"))
    kml = kml_api if (kml_api and kml_api > 0) else (
        (km_rod / litros) if (km_rod and litros and litros > 0) else None
    )
    valor = _br_float(raw.get("valor"))

    return {
        "Data/Hora":       _parse_date_iso(
                               str(raw.get("data", "") or ""),
                               str(raw.get("hora", "") or ""),
                           ),
        "Placa":           str(raw.get("placa", "") or "").upper().strip(),
        "Condutor":        str(raw.get("condutor", "") or "").strip(),
        "Marca":           str(raw.get("marca", "") or "").strip(),
        "Modelo":          str(raw.get("modelo", "") or "").strip(),
        "Ano":             str(raw.get("ano_veiculo", "") or "").strip(),
        "Ult. km":         km_ant,
        "km Atual":        km_atual,
        "km/L":            kml,
        "Km Rodado":       km_rod,
        "Qtde (L)":        litros,
        "Vr. Unit.":       _br_float(raw.get("valorLitro")),
        "Valor":           valor,
        "Produto":         str(raw.get("nomeServico", "") or raw.get("combustivel", "") or "").strip(),
        "Unidade":         str(raw.get("centroDeCustoVeiculo", "") or "").strip(),
        "Estabelecimento": str(raw.get("posto", "") or "").strip(),
        "Registro":        str(raw.get("registroCondutor", "") or "").strip(),
        "Prefixo":         str(raw.get("prefixo", "") or "").strip(),
        "Tipo Frota":      str(raw.get("TipoFrota", "") or "").strip(),
        "R$/km":           ((valor / km_rod) if (valor and km_rod and km_rod > 0) else None),
    }


class SisatecClient:
    """
    Cliente para a API Sisatec de abastecimento.

    Gerencia autenticação, paginação automática e conversão de registros.

    Uso:
        client = SisatecClient()
        records = client.fetch(data_inicio, data_fim)
    """

    _TIMEOUT_SECONDS = 30

    def __init__(
        self,
        base_url: str | None = None,
        codigo: str | None = None,
        key: str | None = None,
    ) -> None:
        """
        Inicializa o cliente com as credenciais fornecidas ou do settings.

        Args:
            base_url: URL base da API (opcional, usa settings.api_base_url).
            codigo:   Código do cliente (opcional, usa settings.api_codigo).
            key:      Chave de autenticação (opcional, usa settings.api_key).
        """
        self._base_url = (base_url or settings.api_base_url).rstrip("/")
        self._codigo = codigo or settings.api_codigo
        self._key = key or settings.api_key

    def _build_url(
        self, data_inicio: datetime.date, data_fim: datetime.date, page: int = 1
    ) -> str:
        """Constrói a URL paginada da API."""
        di = data_inicio.strftime("%m-%d-%Y")
        df = data_fim.strftime("%m-%d-%Y")
        base = f"{self._base_url}/{self._codigo}/{self._key}/{di}/{df}"
        return base if page == 1 else f"{base}?pagina={page}"

    def _get_page(self, url: str) -> dict | list:
        """Realiza uma requisição GET e retorna o JSON decodificado."""
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=self._TIMEOUT_SECONDS) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _extract_records(self, response: dict | list) -> list[dict]:
        """Extrai a lista de registros de uma resposta da API (qualquer formato)."""
        if isinstance(response, list):
            return response
        for key in ("abastecimentos", "dados", "data", "registros"):
            if key in response:
                return response[key]
        return []

    def _total_pages(self, response: dict | list) -> int:
        """Extrai o número total de páginas da resposta."""
        if isinstance(response, list):
            return 1
        return int(response.get("total_paginas", response.get("totalPaginas", 1)) or 1)

    def fetch(
        self,
        data_inicio: datetime.date,
        data_fim: datetime.date,
    ) -> list[dict]:
        """
        Busca todos os registros do período, percorrendo todas as páginas.

        Args:
            data_inicio: Data de início (inclusive).
            data_fim:    Data de fim (inclusive).

        Returns:
            Lista de dicionários no formato interno (colunas do banco).

        Raises:
            urllib.error.URLError:   Falha de rede.
            json.JSONDecodeError:    Resposta inválida da API.
        """
        first = self._get_page(self._build_url(data_inicio, data_fim, page=1))
        all_raw = list(self._extract_records(first))
        total = self._total_pages(first)

        for page in range(2, total + 1):
            page_resp = self._get_page(self._build_url(data_inicio, data_fim, page=page))
            all_raw.extend(self._extract_records(page_resp))

        return [_record_from_dict(r) for r in all_raw]
