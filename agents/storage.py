import sqlite3
import os
import pandas as pd

# Bancos protegidos contra escrita — os agentes jamais devem modificá-los.
_READONLY_DB_NAMES = {"relatorio.db"}


def _assert_not_readonly(db_path: str) -> None:
    """Lança RuntimeError se db_path aponta para um banco protegido."""
    nome = os.path.basename(os.path.abspath(db_path))
    if nome in _READONLY_DB_NAMES:
        raise RuntimeError(
            f"OPERAÇÃO BLOQUEADA: os agentes de auditoria não têm permissão para "
            f"escrever ou modificar '{nome}'. Use um banco separado para resultados."
        )


def _q(nome: str) -> str:
    """Quote de identificador SQL (SQLite) com escape seguro."""
    return '"' + str(nome).replace('"', '""') + '"'


def _sanitize_storage_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove colunas artificiais de planilha/export (ex.: Unnamed: 0)
    e normaliza duplicidades antes de persistir.
    """
    if df is None:
        return pd.DataFrame()

    clean = df.copy()
    clean.columns = [str(c).strip() for c in clean.columns]

    # Descarta colunas tipicas de indice exportado para Excel/CSV.
    cols_drop = [
        c for c in clean.columns
        if c.lower().startswith('unnamed:')
    ]
    if cols_drop:
        clean = clean.drop(columns=cols_drop, errors='ignore')

    # Consolida colunas duplicadas preservando o primeiro nao-nulo por linha.
    if clean.columns.duplicated().any():
        for nome_coluna in clean.columns[clean.columns.duplicated()].unique():
            bloco = clean.loc[:, clean.columns == nome_coluna]
            clean[nome_coluna] = bloco.bfill(axis=1).iloc[:, 0]
        clean = clean.loc[:, ~clean.columns.duplicated()].copy()

    return clean


class AgentStorageSQLite:
    """
    Agente de persistencia local.
    Salva a base normalizada/validada em SQLite e recarrega para uso no pipeline.
    """

    def salvar_e_recarregar(
        self,
        df: pd.DataFrame,
        db_path: str = "auditoria_frota.db",
        table_name: str = "abastecimentos_validados",
    ) -> tuple:
        # Bloqueia escrita em bancos protegidos (ex.: relatorio.db) — sempre, mesmo sem dados.
        _assert_not_readonly(db_path)

        if df is None or df.empty:
            info = {
                'db_path': db_path,
                'requested_db_path': os.path.abspath(db_path),
                'table_name': table_name,
                'rows_written': 0,
                'rows_loaded': 0,
                'status': 'SEM_DADOS',
                'fallback_used': False,
                'fallback_reason': '',
            }
            return df.copy() if df is not None else pd.DataFrame(), info

        df_store = _sanitize_storage_df(df)

        # SQLite nao possui tipo datetime nativo; armazenamos como texto ISO.
        for col in df_store.columns:
            if pd.api.types.is_datetime64_any_dtype(df_store[col]):
                df_store[col] = df_store[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        caminho_principal = os.path.abspath(db_path)
        db_dir = os.path.dirname(caminho_principal)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        conn = None
        try:
            conn = sqlite3.connect(caminho_principal)
            df_store.to_sql(table_name, conn, if_exists='replace', index=False)
            df_db = pd.read_sql_query(f'SELECT * FROM {_q(table_name)}', conn)
        except sqlite3.OperationalError as err:
            if conn is not None:
                conn.close()
            info = {
                'db_path': caminho_principal,
                'requested_db_path': caminho_principal,
                'table_name': table_name,
                'rows_written': int(len(df_store)),
                'rows_loaded': 0,
                'status': 'ERRO_ESCRITA',
                'fallback_used': False,
                'fallback_reason': str(err),
            }
            return df.copy(), info
        finally:
            if conn is not None:
                conn.close()

        if 'data_hora' in df_db.columns:
            df_db['data_hora'] = pd.to_datetime(df_db['data_hora'], errors='coerce')
            df_db['hora'] = df_db['data_hora'].dt.hour
            df_db['dia_semana'] = df_db['data_hora'].dt.dayofweek
            df_db['data_dia'] = df_db['data_hora'].dt.date

        info = {
            'db_path': caminho_principal,
            'requested_db_path': caminho_principal,
            'table_name': table_name,
            'rows_written': int(len(df_store)),
            'rows_loaded': int(len(df_db)),
            'status': 'OK',
            'fallback_used': False,
            'fallback_reason': '',
        }
        return df_db, info

    def append_historico(
        self,
        df: pd.DataFrame,
        db_path: str = "auditoria_frota.db",
        table_name: str = "abastecimentos_historico",
    ) -> tuple:
        """Faz append deduplicado no historico. Mantem registros antigos e insere apenas novos."""
        # Bloqueia escrita em bancos protegidos (ex.: relatorio.db) — sempre, mesmo sem dados.
        _assert_not_readonly(db_path)

        if df is None or df.empty:
            info = {
                'db_path': os.path.abspath(db_path),
                'requested_db_path': os.path.abspath(db_path),
                'table_name': table_name,
                'rows_written': 0,
                'rows_inserted': 0,
                'rows_loaded': 0,
                'status': 'SEM_DADOS',
                'fallback_used': False,
                'fallback_reason': '',
            }
            return df.copy() if df is not None else pd.DataFrame(), info

        df_store = _sanitize_storage_df(df)

        for col in df_store.columns:
            if pd.api.types.is_datetime64_any_dtype(df_store[col]):
                df_store[col] = df_store[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        caminho_principal = os.path.abspath(db_path)
        db_dir = os.path.dirname(caminho_principal)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        conn = None

        # Chave de deduplicacao do historico.
        chave_padrao = ['placa', 'data_hora', 'litros', 'valor_total', 'estabelecimento']
        chave_existente = [c for c in chave_padrao if c in df_store.columns]

        colunas = [c for c in df_store.columns]
        colunas_sql = ', '.join([_q(c) for c in colunas])
        tmp_table = '__tmp_import_hist__'

        def _executar(caminho_db: str):
            nonlocal conn
            conn = sqlite3.connect(caminho_db)
            cur = conn.cursor()

            # Carrega lote em tabela temporaria
            df_store.to_sql(tmp_table, conn, if_exists='replace', index=False)

            # Cria tabela principal se ainda nao existir
            cur.execute(
                f'CREATE TABLE IF NOT EXISTS {_q(table_name)} AS '
                f'SELECT * FROM {_q(tmp_table)} WHERE 1=0'
            )

            # Evolucao de schema: se o lote tiver novas colunas, adiciona na tabela.
            cols_existentes = [r[1] for r in cur.execute(f'PRAGMA table_info({_q(table_name)})').fetchall()]
            for c in colunas:
                if c not in cols_existentes:
                    cur.execute(f'ALTER TABLE {_q(table_name)} ADD COLUMN {_q(c)} TEXT')

            pre_count = cur.execute(f'SELECT COUNT(*) FROM {_q(table_name)}').fetchone()[0]

            if chave_existente:
                idx_nome = f'ux_{table_name}_dedup'
                idx_cols = ', '.join([_q(c) for c in chave_existente])
                cur.execute(
                    f'CREATE UNIQUE INDEX IF NOT EXISTS {_q(idx_nome)} '
                    f'ON {_q(table_name)} ({idx_cols})'
                )

            # Indices operacionais para acelerar filtros e ordenacoes comuns no dashboard.
            if 'placa' in colunas and 'data_hora' in colunas:
                cur.execute(
                    f'CREATE INDEX IF NOT EXISTS {_q(f"ix_{table_name}_placa_data") } '
                    f'ON {_q(table_name)} ({_q("placa")}, {_q("data_hora")})'
                )
            if 'condutor' in colunas:
                cur.execute(
                    f'CREATE INDEX IF NOT EXISTS {_q(f"ix_{table_name}_condutor") } '
                    f'ON {_q(table_name)} ({_q("condutor")})'
                )
            if 'estabelecimento' in colunas:
                cur.execute(
                    f'CREATE INDEX IF NOT EXISTS {_q(f"ix_{table_name}_estabelecimento") } '
                    f'ON {_q(table_name)} ({_q("estabelecimento")})'
                )

                cur.execute(
                    f'INSERT OR IGNORE INTO {_q(table_name)} ({colunas_sql}) '
                    f'SELECT {colunas_sql} FROM {_q(tmp_table)}'
                )
            else:
                cur.execute(
                    f'INSERT INTO {_q(table_name)} ({colunas_sql}) '
                    f'SELECT {colunas_sql} FROM {_q(tmp_table)}'
                )

            post_count = cur.execute(f'SELECT COUNT(*) FROM {_q(table_name)}').fetchone()[0]
            cur.execute(f'DROP TABLE IF EXISTS {_q(tmp_table)}')
            conn.commit()

            df_db = pd.read_sql_query(f'SELECT * FROM {_q(table_name)}', conn)
            return pre_count, post_count, df_db

        try:
            pre_count, post_count, df_db = _executar(caminho_principal)
        except sqlite3.OperationalError as err:
            if conn is not None:
                conn.close()
            info = {
                'db_path': caminho_principal,
                'requested_db_path': caminho_principal,
                'table_name': table_name,
                'rows_written': int(len(df_store)),
                'rows_inserted': 0,
                'rows_loaded': 0,
                'status': 'ERRO_ESCRITA',
                'fallback_used': False,
                'fallback_reason': str(err),
            }
            return pd.DataFrame(), info
        finally:
            if conn is not None:
                conn.close()

        if 'data_hora' in df_db.columns:
            df_db['data_hora'] = pd.to_datetime(df_db['data_hora'], errors='coerce')
            df_db['hora'] = df_db['data_hora'].dt.hour
            df_db['dia_semana'] = df_db['data_hora'].dt.dayofweek
            df_db['data_dia'] = df_db['data_hora'].dt.date

        info = {
            'db_path': caminho_principal,
            'requested_db_path': caminho_principal,
            'table_name': table_name,
            'rows_written': int(len(df_store)),
            'rows_inserted': int(post_count - pre_count),
            'rows_loaded': int(len(df_db)),
            'status': 'OK',
            'fallback_used': False,
            'fallback_reason': '',
        }
        return df_db, info

    def carregar_tabela(
        self,
        db_path: str = "auditoria_frota.db",
        table_name: str = "abastecimentos_historico",
    ) -> tuple:
        """Carrega uma tabela do SQLite para DataFrame."""
        caminho_db = os.path.abspath(db_path)
        if not os.path.exists(caminho_db):
            info = {
                'db_path': caminho_db,
                'table_name': table_name,
                'rows_loaded': 0,
                'status': 'ARQUIVO_AUSENTE',
            }
            return pd.DataFrame(), info

        conn = sqlite3.connect(caminho_db)
        try:
            existe = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            ).fetchone()
            if not existe:
                info = {
                    'db_path': caminho_db,
                    'table_name': table_name,
                    'rows_loaded': 0,
                    'status': 'TABELA_AUSENTE',
                }
                return pd.DataFrame(), info

            df = pd.read_sql_query(f'SELECT * FROM {_q(table_name)}', conn)
        finally:
            conn.close()

        if 'data_hora' in df.columns:
            df['data_hora'] = pd.to_datetime(df['data_hora'], errors='coerce')
            df['hora'] = df['data_hora'].dt.hour
            df['dia_semana'] = df['data_hora'].dt.dayofweek
            df['data_dia'] = df['data_hora'].dt.date

        info = {
            'db_path': caminho_db,
            'table_name': table_name,
            'rows_loaded': int(len(df)),
            'status': 'OK',
        }
        return df, info
