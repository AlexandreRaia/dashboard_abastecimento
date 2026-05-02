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
