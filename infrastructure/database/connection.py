"""
Gerenciamento de conexão com o banco de dados SQLite.

Fornece um context manager thread-safe para abertura de conexões,
garantindo que o banco seja fechado e o commit/rollback seja realizado
automaticamente ao sair do bloco `with`.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator


@contextmanager
def get_connection(db_path: Path) -> Generator[sqlite3.Connection, None, None]:
    """
    Abre uma conexão SQLite e faz commit automático ao sair.

    Em caso de exceção, realiza rollback e repropaga o erro.

    Uso:
        from infrastructure.database.connection import get_connection
        with get_connection(settings.db_path) as conn:
            conn.execute("SELECT ...")

    Args:
        db_path: Caminho absoluto para o arquivo .db.

    Yields:
        sqlite3.Connection com row_factory = sqlite3.Row.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    # Ativa WAL para melhor concorrência de leitura/escrita
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
