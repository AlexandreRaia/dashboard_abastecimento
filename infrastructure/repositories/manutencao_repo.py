"""
Repositório de manutenção — leitura e escrita na tabela gastos_manutencao
e na tabela parametros_manutencao do banco principal (relatorio.db).
"""
from __future__ import annotations

import datetime
import sqlite3
from pathlib import Path

import pandas as pd

from infrastructure.database.connection import get_connection
from infrastructure.database.migrations import ensure_schema

# Mapeamento de cabeçalhos aceitos na planilha → nome interno
_COL_MAP: dict[str, str] = {
    # identificador
    "nro":                  "nro",
    "NRO":                  "nro",
    "Nro":                  "nro",
    # período
    "ano":                  "ano",
    "ANO":                  "ano",
    "mes":                  "mes",
    "N_MÊS":                "mes",
    "N_MES":                "mes",
    "N MÊS":                "mes",
    "N MES":                "mes",
    "n_mes":                "n_mes",
    "MÊS":                  "n_mes",
    "MES":                  "n_mes",
    "Mês":                  "n_mes",
    # veículo
    "placa":                "placa",
    "Placa":                "placa",
    "PLACA":                "placa",
    "marca":                "marca",
    "MARCA":                "marca",
    "modelo":               "modelo",
    "MODELO":               "modelo",
    "combustivel":          "combustivel",
    "COMBUSTIVEL":          "combustivel",
    "COMBUSTÍVEL":          "combustivel",
    # secretaria / custo
    "secretaria":           "secretaria",
    "SECRETARIA":           "secretaria",
    "centro_custo":         "centro_custo",
    "C. CUSTO":             "centro_custo",
    "C.CUSTO":              "centro_custo",
    "C.CUSTO ":             "centro_custo",
    # datas / km
    "data_entrada":         "data_entrada",
    "DATA ENTRADA":         "data_entrada",
    "km_entrada":           "km_entrada",
    "KM ENTRADA":           "km_entrada",
    "data_saida":           "data_saida",
    "DATA SAIDA":           "data_saida",
    "DATA SAÍDA":           "data_saida",
    "km_saida":             "km_saida",
    "KM SAIDA":             "km_saida",
    "KM SAÍDA":             "km_saida",
    "qtd_dias":             "qtd_dias",
    "QTD DIAS":             "qtd_dias",
    # descrição
    "resumo_problema":      "resumo_problema",
    "RESUMO PROBLEMA":      "resumo_problema",
    # orçamento
    "orc":                  "orc",
    "ORÇ":                  "orc",
    "ORC":                  "orc",
    # valores financeiros
    "hmo":                  "hmo",
    "HMO":                  "hmo",
    "vlr_mo":               "vlr_mo",
    "VLR MO":               "vlr_mo",
    "VLR_MO":               "vlr_mo",
    "vlr_pecas":            "vlr_pecas",
    "VLR PEÇAS":            "vlr_pecas",
    "VLR PECAS":            "vlr_pecas",
    "VLR_PECAS":            "vlr_pecas",
    "total":                "total",
    "TOTAL":                "total",
    "vlr_inicial":          "vlr_inicial",
    "VLR INICIAL":          "vlr_inicial",
    "desconto":             "desconto",
    "DESCONTO":             "desconto",
    "pct_fipe":             "pct_fipe",
    "%FIPE":                "pct_fipe",
    "FIPE":                 "pct_fipe",
    # status
    "status_orcamento":     "status_orcamento",
    "Status Orçamento":     "status_orcamento",
    "STATUS ORCAMENTO":     "status_orcamento",
    "STATUS ORÇAMENTO":     "status_orcamento",
    "status_manutencao":    "status_manutencao",
    "STATUS MANUTENÃO":     "status_manutencao",
    "STATUS MANUTENÇÃO":    "status_manutencao",
    "STATUS MANUTENCAO":    "status_manutencao",
    # notas fiscais
    "nf_servicos":          "nf_servicos",
    "NF SERVIÇOS":          "nf_servicos",
    "NF SERVICOS":          "nf_servicos",
    "nf_pecas":             "nf_pecas",
    "NF PEÇAS":             "nf_pecas",
    "NF PECAS":             "nf_pecas",
    # emissão
    "data_emissao":         "data_emissao",
    "DATA  EMISSÃO":        "data_emissao",
    "DATA EMISSÃO":         "data_emissao",
    "DATA EMISSAO":         "data_emissao",
}

_REQUIRED = {"placa", "ano", "mes"}

# Colunas monetárias que precisam de conversão R$ → float
_MONEY_COLS = ("hmo", "vlr_mo", "vlr_pecas", "total", "vlr_inicial", "desconto")

# Colunas de texto simples
_TEXT_COLS = (
    "n_mes", "marca", "modelo", "combustivel", "secretaria", "centro_custo",
    "data_entrada", "data_saida", "resumo_problema", "orc",
    "status_orcamento", "status_manutencao",
    "nf_servicos", "nf_pecas", "data_emissao",
)


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza colunas e tipos do DataFrame importado."""
    df = df.rename(columns={c: _COL_MAP[c] for c in df.columns if c in _COL_MAP})
    missing = _REQUIRED - set(df.columns)
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing}")

    # Valores monetários
    for col in _MONEY_COLS:
        if col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                # Excel já entregou como float — usa direto, sem tocar no ponto decimal
                df[col] = df[col].fillna(0.0)
            else:
                # String no formato BRL: "R$ 1.234,56" → remove R$, remove sep. milhar, troca vírgula
                df[col] = pd.to_numeric(
                    df[col].astype(str)
                      .str.replace(r"[R$\s]", "", regex=True)
                      .str.replace(r"\.(?=\d{3})", "", regex=True)
                      .str.replace(",", ".", regex=False),
                    errors="coerce",
                ).fillna(0.0)

    # Se total não vier na planilha, calcula
    if "total" not in df.columns:
        df["total"] = df.get("vlr_mo", pd.Series(0.0, index=df.index)) + \
                      df.get("vlr_pecas", pd.Series(0.0, index=df.index))

    # Inteiros
    for col in ("ano", "mes"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in ("nro", "qtd_dias"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in ("km_entrada", "km_saida", "pct_fipe"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Textos
    for col in _TEXT_COLS:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()

    # Normaliza combustivel
    if "combustivel" in df.columns:
        comb = df["combustivel"].str.upper().str.strip()
        df["combustivel"] = comb.map(
            lambda v: "DIESEL S10" if "DIESEL" in v
            else ("FLEX/GAS" if v in ("FLEX", "GASOLINA", "") else v)
        )

    df["importado_em"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df


def load_gastos(db_path: Path) -> pd.DataFrame:
    """Carrega todos os registros de gastos_manutencao."""
    ensure_schema(db_path)
    with get_connection(db_path) as conn:
        return pd.read_sql_query(
            "SELECT * FROM gastos_manutencao ORDER BY ano, mes", conn
        )


def load_from_gsheets(csv_url: str) -> pd.DataFrame:
    """
    Lê a planilha publicada no Google Sheets como CSV e retorna um DataFrame bruto.
    Levanta ValueError se a URL estiver vazia ou a requisição falhar.
    """
    if not csv_url:
        raise ValueError("URL do Google Sheets não configurada.")
    try:
        df = pd.read_csv(csv_url, encoding="utf-8-sig")
    except Exception as exc:
        raise ValueError(f"Erro ao acessar Google Sheets: {exc}") from exc
    return df


def sync_from_gsheets(db_path: Path, csv_url: str) -> tuple[int, int]:
    """
    Lê a planilha do Google Sheets, normaliza e insere no banco.
    Retorna (inseridos, ignorados).
    """
    df_raw = load_from_gsheets(csv_url)
    return insert_gastos_df(db_path, df_raw)



def get_gastos_by_period(
    db_path: Path,
    ano_ini: int | None = None,
    mes_ini: int | None = None,
    ano_fim: int | None = None,
    mes_fim: int | None = None,
    secretarias: list[str] | None = None,
) -> pd.DataFrame:
    """Filtra gastos por período e secretaria."""
    df = load_gastos(db_path)
    if df.empty:
        return df

    df["_ym"] = df["ano"] * 100 + df["mes"]
    if ano_ini and mes_ini:
        df = df[df["_ym"] >= ano_ini * 100 + mes_ini]
    if ano_fim and mes_fim:
        df = df[df["_ym"] <= ano_fim * 100 + mes_fim]
    if secretarias:
        df = df[df["secretaria"].isin(secretarias)]

    return df.drop(columns=["_ym"])


def insert_gastos_df(db_path: Path, df: pd.DataFrame) -> tuple[int, int]:
    """
    Insere registros no banco, ignorando duplicatas exatas
    (placa + ano + mes + vlr_mo + vlr_pecas).

    Retorna (inseridos, ignorados).
    """
    ensure_schema(db_path)
    df_norm = _normalize_df(df.copy())

    _COLS = [
        "nro", "ano", "mes", "n_mes", "placa", "marca", "modelo", "combustivel",
        "secretaria", "data_entrada", "km_entrada", "data_saida", "km_saida",
        "qtd_dias", "resumo_problema", "centro_custo", "orc",
        "hmo", "vlr_mo", "vlr_pecas", "total",
        "vlr_inicial", "desconto", "pct_fipe",
        "status_orcamento", "status_manutencao",
        "nf_servicos", "nf_pecas", "data_emissao",
        "importado_em",
    ]
    df_norm = df_norm[[c for c in _COLS if c in df_norm.columns]]

    inserted = 0
    ignored  = 0

    with get_connection(db_path) as conn:
        existing = pd.read_sql_query(
            "SELECT placa, ano, mes, vlr_mo, vlr_pecas FROM gastos_manutencao", conn
        )
        existing["_key"] = (
            existing["placa"].astype(str)
            + "|" + existing["ano"].astype(str)
            + "|" + existing["mes"].astype(str)
            + "|" + existing["vlr_mo"].astype(str)
            + "|" + existing["vlr_pecas"].astype(str)
        )
        existing_keys = set(existing["_key"])

        for _, row in df_norm.iterrows():
            key = (
                f"{row['placa']}|{row['ano']}|{row['mes']}"
                f"|{row['vlr_mo']}|{row['vlr_pecas']}"
            )
            if key in existing_keys:
                ignored += 1
                continue
            cols = list(row.index)
            placeholders = ", ".join("?" * len(cols))
            conn.execute(
                f"INSERT INTO gastos_manutencao ({', '.join(cols)}) VALUES ({placeholders})",
                list(row.values),
            )
            inserted += 1

    return inserted, ignored


def delete_all_gastos(db_path: Path) -> int:
    """Remove todos os registros de gastos_manutencao. Retorna linhas deletadas."""
    ensure_schema(db_path)
    with get_connection(db_path) as conn:
        cur = conn.execute("DELETE FROM gastos_manutencao")
        return cur.rowcount


# ---------------------------------------------------------------------------
# Empenhos
# ---------------------------------------------------------------------------

def get_parametros_manutencao(db_path: Path) -> pd.DataFrame:
    """Retorna todos os empenhos de manutenção cadastrados."""
    ensure_schema(db_path)
    with get_connection(db_path) as conn:
        return pd.read_sql_query(
            "SELECT * FROM parametros_manutencao ORDER BY secretaria", conn
        )


def save_parametros_manutencao(db_path: Path, df: pd.DataFrame) -> int:
    """
    Upsert dos empenhos de manutenção.
    Retorna número de linhas salvas.
    """
    ensure_schema(db_path)
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    saved = 0
    with get_connection(db_path) as conn:
        for _, row in df.iterrows():
            conn.execute(
                """
                INSERT INTO parametros_manutencao
                    (secretaria, contrato, data_inicio, data_fim, valor_empenhado, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(secretaria, contrato) DO UPDATE SET
                    data_inicio     = excluded.data_inicio,
                    data_fim        = excluded.data_fim,
                    valor_empenhado = excluded.valor_empenhado,
                    updated_at      = excluded.updated_at
                """,
                (
                    str(row.get("secretaria", "")).strip(),
                    str(row.get("contrato", "")).strip(),
                    str(row.get("data_inicio", "")).strip(),
                    str(row.get("data_fim", "")).strip(),
                    float(row.get("valor_empenhado", 0.0)),
                    now,
                ),
            )
            saved += 1
    return saved
