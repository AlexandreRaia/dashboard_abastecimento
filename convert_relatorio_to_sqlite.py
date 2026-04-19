import re
import sqlite3
import pandas as pd

xlsx_path = 'Relatorio.xlsx'
db_path = 'relatorio.db'

def sanitize(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r'\s+', '_', s)
    s = re.sub(r'[^a-z0-9_]', '_', s)
    s = re.sub(r'_+', '_', s).strip('_')
    return s or 'sheet'

sheets = pd.read_excel(xlsx_path, sheet_name=None)
single_sheet = len(sheets) == 1

used = set()
created = []

with sqlite3.connect(db_path) as conn:
    for sheet_name, df in sheets.items():
        base = 'abastecimentos' if single_sheet else sanitize(sheet_name)
        table = base
        i = 1
        while table in used:
            i += 1
            table = f"{base}_{i}"
        used.add(table)

        df.to_sql(table, conn, if_exists='replace', index=False)
        row_count = len(df)
        created.append((table, row_count))
        print(f"CREATED {table} {row_count}")

    print("VERIFY")
    tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", conn)['name'].tolist()
    for t in tables:
        c = pd.read_sql_query(f'SELECT COUNT(*) AS n FROM "{t}"', conn)['n'].iloc[0]
        print(f"TABLE {t} {c}")
