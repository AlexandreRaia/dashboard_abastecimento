"""
Repositório de parâmetros financeiros anuais.

Encapsula todo o acesso CRUD à tabela `parametros_financeiros_anuais`,
isolando queries SQL da camada de negócio.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from infrastructure.database.connection import get_connection


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------

def get_params(db_path: Path, anos: list[int], secretaria: str | None = None) -> pd.DataFrame:
    """
    Lê parâmetros financeiros do banco para os anos e secretaria fornecidos.

    Args:
        db_path:    Caminho do banco de dados.
        anos:       Lista de anos a consultar.
        secretaria: Filtra por secretaria específica se fornecido.

    Returns:
        DataFrame com colunas: secretaria, ano, valor_empenhado,
        limite_litros_gasolina, limite_litros_alcool, limite_litros_diesel,
        desconto_percentual, limite_mensal, limite_litros_mensal.
    """
    if not anos:
        return pd.DataFrame()

    placeholders = ",".join("?" * len(anos))
    query = f"""
        SELECT
            secretaria,
            ano,
            valor_empenhado,
            limite_litros_gasolina,
            limite_litros_alcool,
            limite_litros_diesel,
            desconto_percentual
        FROM parametros_financeiros_anuais
        WHERE ano IN ({placeholders})
    """
    params: list = list(anos)

    if secretaria:
        query += " AND secretaria = ?"
        params.append(secretaria)

    query += " ORDER BY secretaria, ano"

    with get_connection(db_path) as conn:
        df = pd.read_sql_query(query, conn, params=params)

    # Garante ausência de NaN em colunas numéricas
    _num_cols = [
        "valor_empenhado",
        "limite_litros_gasolina",
        "limite_litros_alcool",
        "limite_litros_diesel",
        "desconto_percentual",
    ]
    for col in _num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Colunas derivadas usadas nos serviços de KPI
    df["limite_mensal"] = df["valor_empenhado"] / 12.0
    df["limite_litros_mensal"] = (
        df["limite_litros_gasolina"]
        + df["limite_litros_alcool"]
        + df["limite_litros_diesel"]
    )

    return df


def get_all_params(db_path: Path) -> pd.DataFrame:
    """
    Carrega todos os parâmetros financeiros para o editor da sidebar.

    Returns:
        DataFrame com todos os anos e secretarias cadastrados,
        com coluna 'desconto_pct' (%) pronta para o st.data_editor.
    """
    with get_connection(db_path) as conn:
        df = pd.read_sql_query(
            """
            SELECT
                secretaria,
                ano,
                valor_empenhado,
                limite_litros_gasolina,
                limite_litros_alcool,
                limite_litros_diesel,
                desconto_percentual
            FROM parametros_financeiros_anuais
            ORDER BY secretaria, ano
            """,
            conn,
        )

    if df.empty:
        return pd.DataFrame(
            columns=[
                "secretaria", "ano", "valor_empenhado",
                "limite_litros_gasolina", "limite_litros_alcool",
                "limite_litros_diesel", "desconto_pct",
            ]
        )

    _num_cols = [
        "valor_empenhado", "limite_litros_gasolina",
        "limite_litros_alcool", "limite_litros_diesel", "desconto_percentual",
    ]
    for col in _num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").fillna(0).astype(int)
    df["secretaria"] = df["secretaria"].astype(str).str.strip().str.upper()
    # Expõe desconto em % para facilitar edição
    df["desconto_pct"] = df["desconto_percentual"] * 100.0
    df = df.drop(columns=["desconto_percentual"])
    return df


# ---------------------------------------------------------------------------
# Escrita
# ---------------------------------------------------------------------------

def save_params(db_path: Path, df_editor: pd.DataFrame) -> tuple[int, int]:
    """
    Persiste alterações do editor (insert / update / delete).

    Usa INSERT OR REPLACE para upsert e DELETE para linhas removidas.

    Args:
        db_path:    Caminho do banco de dados.
        df_editor:  DataFrame com colunas: secretaria, ano, valor_empenhado,
                    limite_litros_gasolina, limite_litros_alcool,
                    limite_litros_diesel, desconto_pct.

    Returns:
        Tupla (linhas_salvas, linhas_deletadas).

    Raises:
        ValueError: Se houver dados inválidos (negativos, duplicados, etc.).
    """
    required_cols = [
        "secretaria", "ano", "valor_empenhado",
        "limite_litros_gasolina", "limite_litros_alcool",
        "limite_litros_diesel", "desconto_pct",
    ]
    missing = [c for c in required_cols if c not in df_editor.columns]
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes: {', '.join(missing)}")

    df = df_editor[required_cols].copy()
    df["secretaria"] = df["secretaria"].astype(str).str.strip().str.upper()
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce")

    for col in ["valor_empenhado", "limite_litros_gasolina",
                "limite_litros_alcool", "limite_litros_diesel", "desconto_pct"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove linhas completamente vazias geradas pelo data_editor
    empty = (
        df["secretaria"].eq("")
        & df["ano"].isna()
        & df[["valor_empenhado", "limite_litros_gasolina",
              "limite_litros_alcool", "limite_litros_diesel", "desconto_pct"]].isna().all(axis=1)
    )
    df = df[~empty].copy()

    # Validações
    if df["secretaria"].eq("").any():
        raise ValueError("A coluna 'secretaria' não pode estar vazia.")
    if df["ano"].isna().any():
        raise ValueError("A coluna 'ano' deve ser preenchida em todas as linhas.")

    df["ano"] = df["ano"].astype(int)
    if ((df["ano"] < 2000) | (df["ano"] > 2100)).any():
        raise ValueError("Ano inválido. Use valores entre 2000 e 2100.")

    for col in ["valor_empenhado", "limite_litros_gasolina",
                "limite_litros_alcool", "limite_litros_diesel", "desconto_pct"]:
        df[col] = df[col].fillna(0.0)
        if (df[col] < 0).any():
            raise ValueError(f"Valores negativos não são permitidos em '{col}'.")

    if (df["desconto_pct"] > 100).any():
        raise ValueError("Desconto (%) deve estar entre 0 e 100.")

    dupes = df.duplicated(subset=["secretaria", "ano"], keep=False)
    if dupes.any():
        keys = sorted(
            {f"{r.secretaria}/{r.ano}" for r in df.loc[dupes, ["secretaria", "ano"]].itertuples()}
        )
        raise ValueError("Chaves duplicadas (secretaria/ano): " + ", ".join(keys))

    # Converte % → fração decimal para persistência
    df["desconto_percentual"] = (df["desconto_pct"] / 100.0).astype(float)

    with get_connection(db_path) as conn:
        current_keys = {
            (row[0], row[1])
            for row in conn.execute(
                "SELECT secretaria, ano FROM parametros_financeiros_anuais"
            )
        }
        new_keys = {(row.secretaria, int(row.ano)) for row in df[["secretaria", "ano"]].itertuples()}
        to_delete = current_keys - new_keys

        for row in df.itertuples(index=False):
            conn.execute(
                """
                INSERT INTO parametros_financeiros_anuais (
                    secretaria, ano, valor_empenhado,
                    limite_litros_gasolina, limite_litros_alcool,
                    limite_litros_diesel, desconto_percentual, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(secretaria, ano) DO UPDATE SET
                    valor_empenhado         = excluded.valor_empenhado,
                    limite_litros_gasolina  = excluded.limite_litros_gasolina,
                    limite_litros_alcool    = excluded.limite_litros_alcool,
                    limite_litros_diesel    = excluded.limite_litros_diesel,
                    desconto_percentual     = excluded.desconto_percentual,
                    updated_at              = CURRENT_TIMESTAMP
                """,
                (
                    row.secretaria, int(row.ano),
                    float(row.valor_empenhado), float(row.limite_litros_gasolina),
                    float(row.limite_litros_alcool), float(row.limite_litros_diesel),
                    float(row.desconto_percentual),
                ),
            )

        if to_delete:
            conn.executemany(
                "DELETE FROM parametros_financeiros_anuais WHERE secretaria = ? AND ano = ?",
                list(to_delete),
            )

    return len(df), len(to_delete)
