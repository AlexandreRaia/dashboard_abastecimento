"""
Repositório de resoluções de ocorrências de auditoria.

Persiste e recupera o feedback do auditor (JUSTIFICADA / DESCARTADA)
para cada ocorrência identificada pelo seu UUID determinístico.

Banco: auditoria_resultados.db  (leitura/escrita)
Tabela: resolucoes
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from infrastructure.database.connection import get_connection


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS resolucoes (
    id_ocorrencia  TEXT PRIMARY KEY,
    status         TEXT NOT NULL CHECK(status IN ('JUSTIFICADA','DESCARTADA')),
    observacao     TEXT NOT NULL DEFAULT '',
    resolvido_em   TEXT NOT NULL
);
"""


def ensure_resolucoes_table(db_path: Path) -> None:
    """Cria a tabela resolucoes se não existir. Idempotente."""
    with get_connection(db_path) as conn:
        conn.execute(_DDL)


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------

def get_resolucoes(db_path: Path) -> dict[str, dict]:
    """
    Retorna dict { id_ocorrencia -> {status, observacao, resolvido_em} }
    para todas as resoluções gravadas.
    """
    ensure_resolucoes_table(db_path)
    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT id_ocorrencia, status, observacao, resolvido_em FROM resolucoes"
        ).fetchall()
    return {
        r[0]: {"status": r[1], "observacao": r[2], "resolvido_em": r[3]}
        for r in rows
    }


# ---------------------------------------------------------------------------
# Escrita
# ---------------------------------------------------------------------------

def salvar_resolucao(
    db_path: Path,
    id_ocorrencia: str,
    status: str,
    observacao: str = "",
) -> None:
    """
    Insere ou atualiza a resolução de uma ocorrência.

    Args:
        db_path:       Caminho para auditoria_resultados.db
        id_ocorrencia: UUID5 determinístico da ocorrência
        status:        'JUSTIFICADA' ou 'DESCARTADA'
        observacao:    Texto livre do auditor
    """
    ensure_resolucoes_table(db_path)
    agora = datetime.now().isoformat(timespec="seconds")
    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO resolucoes (id_ocorrencia, status, observacao, resolvido_em)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id_ocorrencia) DO UPDATE SET
                status       = excluded.status,
                observacao   = excluded.observacao,
                resolvido_em = excluded.resolvido_em
            """,
            (id_ocorrencia, status, observacao, agora),
        )


def remover_resolucao(db_path: Path, id_ocorrencia: str) -> None:
    """Remove a resolução de uma ocorrência (re-abre como pendente)."""
    ensure_resolucoes_table(db_path)
    with get_connection(db_path) as conn:
        conn.execute(
            "DELETE FROM resolucoes WHERE id_ocorrencia = ?", (id_ocorrencia,)
        )
