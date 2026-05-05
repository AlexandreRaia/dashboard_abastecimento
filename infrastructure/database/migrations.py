"""
Migrações do banco de dados principal (relatorio.db).

Centraliza a criação e evolução do schema para que nenhuma página
ou agente precise duplicar DDL. Execute ensure_schema() na inicialização
da aplicação.
"""
from __future__ import annotations

from pathlib import Path
from infrastructure.database.connection import get_connection


def ensure_schema(db_path: Path) -> None:
    """
    Garante que todas as tabelas e colunas necessárias existam.

    Idempotente: seguro para chamar múltiplas vezes.
    Novas colunas são adicionadas via ALTER TABLE sem perder dados.

    Args:
        db_path: Caminho para o banco de dados principal.
    """
    with get_connection(db_path) as conn:
        # ------------------------------------------------------------------
        # Tabela de parâmetros financeiros anuais
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS parametros_financeiros_anuais (
                secretaria              TEXT    NOT NULL,
                ano                     INTEGER NOT NULL,
                valor_empenhado         REAL    DEFAULT 0.0,
                limite_litros_gasolina  REAL    DEFAULT 0.0,
                limite_litros_alcool    REAL    DEFAULT 0.0,
                limite_litros_diesel    REAL    DEFAULT 0.0,
                desconto_percentual     REAL    DEFAULT 0.0,
                updated_at              TEXT    DEFAULT NULL,
                PRIMARY KEY (secretaria, ano)
            )
            """
        )

        # Migração incremental: adiciona colunas ausentes em bancos antigos
        existing_cols = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(parametros_financeiros_anuais)"
            )
        }
        _migrations = [
            ("updated_at", "TEXT DEFAULT NULL"),
        ]
        for col_name, col_def in _migrations:
            if col_name not in existing_cols:
                conn.execute(
                    f"ALTER TABLE parametros_financeiros_anuais "
                    f"ADD COLUMN {col_name} {col_def}"
                )

        # ------------------------------------------------------------------
        # Tabela de gastos de manutenção
        # Migration: se tabela existe com schema antigo (sem data_entrada), recria
        # ------------------------------------------------------------------
        _manut_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(gastos_manutencao)")
        }
        if _manut_cols and "data_entrada" not in _manut_cols:
            conn.execute("DROP TABLE IF EXISTS gastos_manutencao")
            conn.execute("DROP INDEX IF EXISTS idx_manut_ano_mes")
            _manut_cols = set()

        if not _manut_cols:
            conn.execute(
                """
                CREATE TABLE gastos_manutencao (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    nro               INTEGER,
                    ano               INTEGER NOT NULL,
                    mes               INTEGER NOT NULL,
                    n_mes             TEXT,
                    placa             TEXT    NOT NULL DEFAULT '',
                    marca             TEXT    NOT NULL DEFAULT '',
                    modelo            TEXT    NOT NULL DEFAULT '',
                    combustivel       TEXT    NOT NULL DEFAULT '',
                    secretaria        TEXT    NOT NULL DEFAULT '',
                    data_entrada      TEXT,
                    km_entrada        REAL,
                    data_saida        TEXT,
                    km_saida          REAL,
                    qtd_dias          INTEGER,
                    resumo_problema   TEXT,
                    centro_custo      TEXT    NOT NULL DEFAULT '',
                    orc               TEXT,
                    hmo               REAL    NOT NULL DEFAULT 0.0,
                    vlr_mo            REAL    NOT NULL DEFAULT 0.0,
                    vlr_pecas         REAL    NOT NULL DEFAULT 0.0,
                    total             REAL    NOT NULL DEFAULT 0.0,
                    vlr_inicial       REAL,
                    desconto          REAL,
                    pct_fipe          REAL,
                    status_orcamento  TEXT,
                    status_manutencao TEXT,
                    nf_servicos       TEXT,
                    nf_pecas          TEXT,
                    data_emissao      TEXT,
                    importado_em      TEXT    DEFAULT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_manut_ano_mes
                ON gastos_manutencao (ano, mes, secretaria)
                """
            )

        # ------------------------------------------------------------------
        # Tabela de empenhos de manutenção por secretaria/contrato
        # ------------------------------------------------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS parametros_manutencao (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                secretaria      TEXT    NOT NULL,
                contrato        TEXT    NOT NULL DEFAULT '',
                data_inicio     TEXT    NOT NULL DEFAULT '',
                data_fim        TEXT    NOT NULL DEFAULT '',
                valor_empenhado REAL    NOT NULL DEFAULT 0.0,
                updated_at      TEXT    DEFAULT NULL,
                UNIQUE(secretaria, contrato)
            )
            """
        )

        # Seed: valores do Contrato 103/2023 2° Termo (jan–ago 2026)
        _SEED_PARAMS = [
            ("SMA",  "103/2023", "2026-01-27", "2026-08-31", 204745.00),
            ("SMSU", "103/2023", "2026-01-27", "2026-08-31", 180319.32),
            ("SME",  "103/2023", "2026-01-27", "2026-08-31", 143282.11),
            ("SMS",  "103/2023", "2026-01-27", "2026-08-31", 170116.92),
            ("SMTT", "103/2023", "2026-01-27", "2026-08-31",  36161.65),
        ]
        conn.executemany(
            """
            INSERT OR IGNORE INTO parametros_manutencao
                (secretaria, contrato, data_inicio, data_fim, valor_empenhado)
            VALUES (?, ?, ?, ?, ?)
            """,
            _SEED_PARAMS,
        )
