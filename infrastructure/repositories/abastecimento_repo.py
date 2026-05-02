"""
Repositório de abastecimentos — acesso de leitura/escrita ao banco principal.

Encapsula todas as queries SQL relacionadas aos registros de abastecimento,
isolando a camada de dados da lógica de negócio e da UI.
"""
from __future__ import annotations

import io
import re
import sqlite3
from pathlib import Path

import pandas as pd

from config.constants import MONTHS
from core.services.normalization import normalize_secretaria, normalize_fuel
from agents.config import canonicalizar_modelo, canonicalizar_marca
from infrastructure.database.connection import get_connection


# Colunas esperadas na tabela para considerar como "tabela de abastecimentos"
_KNOWN_COLS: frozenset[str] = frozenset(
    {"Placa", "Data/Hora", "Unidade", "Produto", "Qtde (L)", "Valor"}
)


# ---------------------------------------------------------------------------
# Resolução do nome da tabela no banco
# ---------------------------------------------------------------------------

def _resolve_table(conn: sqlite3.Connection) -> str:
    """
    Detecta qual tabela de abastecimento existe no banco.

    Suporta tanto o esquema legado ('plan1') quanto o novo ('abastecimentos').

    Raises:
        RuntimeError: Se nenhuma tabela compatível for encontrada.
    """
    tables: list[str] = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", conn
    )["name"].tolist()

    if "abastecimentos" in tables:
        return "abastecimentos"
    if "plan1" in tables:
        return "plan1"
    raise RuntimeError(
        "Nenhuma tabela de abastecimento encontrada no banco de dados. "
        "Importe um relatório Excel antes de continuar."
    )


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------

def load_abastecimentos(db_path: Path) -> pd.DataFrame:
    """
    Carrega e normaliza os registros de abastecimento do banco SQLite.

    Operações realizadas:
    - Renomeia colunas para nomes internos padronizados
    - Normaliza secretaria e combustível
    - Converte tipos numéricos e de data
    - Adiciona colunas derivadas: ano, mes, mes_nome, ano_mes

    Args:
        db_path: Caminho para o arquivo relatorio.db.

    Returns:
        DataFrame normalizado pronto para uso nos serviços de KPI.
    """
    with get_connection(db_path) as conn:
        table = _resolve_table(conn)
        query = f"""
            SELECT
                "Data/Hora"        AS data_hora,
                "Unidade"          AS secretaria,
                "Produto"          AS combustivel,
                "Vr. Unit."        AS valor_unitario,
                "Qtde (L)"         AS litros,
                "Valor"            AS valor,
                "Placa"            AS placa,
                "Condutor"         AS condutor,
                "Km Rodado"        AS km_rodado,
                "km/L"             AS km_por_litro,
                "R$/km"            AS custo_por_km,
                "KM Minimo"        AS km_minimo,
                "KM Maximo"        AS km_maximo,
                "Estabelecimento"  AS posto,
                "Marca"            AS marca,
                "Modelo"           AS modelo,
                "Tipo Frota"       AS tipo_frota,
                "Ult. km"          AS ult_km,
                "km Atual"         AS km_atual
            FROM {table}
        """
        df = pd.read_sql_query(query, conn)

    # Datas
    df["data_hora"] = pd.to_datetime(df["data_hora"], errors="coerce")

    # Normalização de texto
    df["secretaria"] = df["secretaria"].map(normalize_secretaria)
    df["combustivel"] = df["combustivel"].map(normalize_fuel)

    # Colunas numéricas
    _num_cols = (
        "valor_unitario", "litros", "valor", "km_rodado",
        "km_por_litro", "custo_por_km", "km_minimo", "km_maximo",
        "ult_km", "km_atual",
    )
    for col in _num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Colunas de texto
    _str_cols = ("placa", "condutor", "posto", "marca", "modelo", "tipo_frota")
    for col in _str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    if "placa" in df.columns:
        df["placa"] = df["placa"].str.upper()

    # Canonicalização de modelo e marca
    if "modelo" in df.columns:
        df["modelo"] = df["modelo"].apply(canonicalizar_modelo)
    if "marca" in df.columns:
        df["marca"] = df["marca"].apply(canonicalizar_marca)

    # Remove linhas sem data válida
    df = df.dropna(subset=["data_hora"])

    # Colunas derivadas de tempo
    df["ano"] = df["data_hora"].dt.year
    df["mes"] = df["data_hora"].dt.month
    df["mes_nome"] = df["mes"].map(MONTHS)
    df["ano_mes"] = df["data_hora"].dt.to_period("M").astype(str)

    # Alias para compatibilidade com código UI que referencia "data"
    df["data"] = df["data_hora"]

    return df


# ---------------------------------------------------------------------------
# Importação de Excel
# ---------------------------------------------------------------------------

def _sanitize_table_name(name: str) -> str:
    """Converte nome de aba do Excel em nome de tabela SQLite válido."""
    s = name.strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "sheet"


def _find_header_row(raw_df: pd.DataFrame) -> int | None:
    """Detecta a linha de cabeçalho no Excel buscando colunas conhecidas."""
    for i, row in raw_df.iterrows():
        row_vals = {str(v).strip() for v in row.values}
        if len(_KNOWN_COLS & row_vals) >= 2:
            return int(i)
    return None


def _safe_df_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
    """Converte colunas Timestamp para string para compatibilidade com SQLite."""
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)
        elif df[col].dtype == object:
            df[col] = df[col].apply(
                lambda v: str(v) if hasattr(v, "isoformat") else v
            )
    return df


def import_excel(file_bytes: bytes, db_path: Path) -> dict[str, int]:
    """
    Importa um arquivo Excel para o banco SQLite, mesclando com dados existentes.

    Deduplica registros por todas as colunas antes de gravar.

    Args:
        file_bytes: Conteúdo binário do arquivo .xlsx / .xls.
        db_path:    Caminho do banco de destino.

    Returns:
        Dicionário {nome_tabela: linhas_gravadas} para cada aba processada.
    """
    sheets_raw: dict[str, pd.DataFrame] = pd.read_excel(
        io.BytesIO(file_bytes), sheet_name=None, header=None
    )
    single_sheet = len(sheets_raw) == 1
    used_tables: set[str] = set()
    result: dict[str, int] = {}

    with get_connection(db_path) as conn:
        # get_connection usa sqlite3.Row — precisa de conexão nua para pd.read_sql
        raw_conn = sqlite3.connect(str(db_path))
        try:
            for sheet_name, raw in sheets_raw.items():
                base = "abastecimentos" if single_sheet else _sanitize_table_name(sheet_name)
                tbl = base
                idx = 1
                while tbl in used_tables:
                    idx += 1
                    tbl = f"{base}_{idx}"
                used_tables.add(tbl)

                header_row = _find_header_row(raw)
                if header_row is not None:
                    df_import = pd.read_excel(
                        io.BytesIO(file_bytes), sheet_name=sheet_name, header=header_row
                    )
                else:
                    df_import = raw

                try:
                    df_existing = pd.read_sql_query(f'SELECT * FROM "{tbl}"', raw_conn)
                except Exception:
                    df_existing = pd.DataFrame()

                df_combined = (
                    pd.concat([df_existing, df_import], ignore_index=True)
                    .drop_duplicates()
                )
                df_combined = _safe_df_for_sqlite(df_combined)
                df_combined.to_sql(tbl, raw_conn, if_exists="replace", index=False)
                raw_conn.commit()
                result[tbl] = len(df_combined)
        finally:
            raw_conn.close()

    return result
