import numpy as np
import pandas as pd

from .config import COLUMN_MAP, normalizar_texto, canonicalizar_modelo


class AgentIngestion:
    """
    Agente 1 — Ingestao e Padronizacao.
    Responsavel por ler, mapear colunas, converter tipos e normalizar textos.
    """

    def processar(self, df_raw: pd.DataFrame) -> tuple:
        """
        Retorna (df_padronizado, relatorio_ingestao).
        """
        relatorio = []
        df = df_raw.copy()
        df.columns = [c.strip() for c in df.columns]

        # --- Mapeamento de colunas ---
        renomear = {col: interno for col, interno in COLUMN_MAP.items() if col in df.columns}
        ausentes = [col for col in COLUMN_MAP if col not in df.columns]
        df = df.rename(columns=renomear)

        # Se o DataFrame ja vier parcialmente processado, o rename pode criar
        # colunas duplicadas (ex.: 'Qtde (L)' -> 'litros' quando 'litros' ja existe).
        # Consolidamos as duplicadas preservando o primeiro valor nao-nulo por linha.
        if df.columns.duplicated().any():
            duplicadas = df.columns[df.columns.duplicated()].unique()
            for nome in duplicadas:
                bloco = df.loc[:, df.columns == nome]
                df[nome] = bloco.bfill(axis=1).iloc[:, 0]
            df = df.loc[:, ~df.columns.duplicated()]

        obrigatorias_internas = {'data_hora', 'placa', 'condutor', 'litros', 'km_rodado', 'modelo'}
        ausentes_obrig = [
            col for col in ausentes
            if COLUMN_MAP.get(col) in obrigatorias_internas
        ]

        if ausentes_obrig:
            relatorio.append({
                'tipo': 'COLUNAS_OBRIGATORIAS_AUSENTES',
                'detalhe': f"Colunas obrigatorias nao encontradas: {', '.join(ausentes_obrig)}",
                'gravidade': 'CRITICA',
            })
        elif ausentes:
            relatorio.append({
                'tipo': 'COLUNAS_OPCIONAIS_AUSENTES',
                'detalhe': f"Colunas opcionais ausentes (analise parcial): {', '.join(ausentes)}",
                'gravidade': 'INFORMATIVO',
            })

        # --- Datas ---
        if 'data_hora' in df.columns:
            df['data_hora'] = self._processar_datas(df['data_hora'])
            nulos = int(df['data_hora'].isna().sum())
            if nulos:
                relatorio.append({
                    'tipo': 'DATAS_INVALIDAS',
                    'detalhe': f"{nulos} registro(s) com data/hora invalida convertidos para nulo.",
                    'gravidade': 'MEDIA',
                })

        # --- Numericos ---
        for col in ['litros', 'km_rodado', 'ult_km', 'km_atual',
                    'valor_unitario', 'valor_total',
                    'km_l_informado', 'km_minimo', 'km_maximo']:
            if col in df.columns:
                df[col] = self._processar_numericos(df[col])

        # --- Consumo ---
        if 'km_l_informado' in df.columns and 'km_rodado' in df.columns and 'litros' in df.columns:
            calculado = df['km_rodado'] / df['litros'].replace(0, np.nan)
            df['consumo'] = df['km_l_informado'].combine_first(calculado)
        elif 'km_rodado' in df.columns and 'litros' in df.columns:
            df['consumo'] = df['km_rodado'] / df['litros'].replace(0, np.nan)
        else:
            df['consumo'] = np.nan

        # --- Textos ---
        for col in ['modelo', 'condutor', 'placa', 'produto',
                    'estabelecimento', 'unidade', 'tipo_frota', 'marca']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()

        if 'modelo' in df.columns:
            df['modelo_original'] = df['modelo']
            df['modelo'] = df['modelo'].apply(canonicalizar_modelo)
            df['modelo_norm'] = df['modelo'].apply(normalizar_texto)

        # --- Campos temporais auxiliares ---
        if 'data_hora' in df.columns:
            df['hora']      = df['data_hora'].dt.hour
            df['dia_semana'] = df['data_hora'].dt.dayofweek
            df['data_dia']  = df['data_hora'].dt.date

        placas  = df['placa'].nunique()       if 'placa'    in df.columns else '?'
        condut  = df['condutor'].nunique()    if 'condutor' in df.columns else '?'
        relatorio.append({
            'tipo': 'INGESTAO_CONCLUIDA',
            'detalhe': (
                f"Base carregada: {len(df)} registros | "
                f"{placas} placas | {condut} condutores."
            ),
            'quantidade': len(df),
            'gravidade': 'INFORMATIVO',
        })

        return df, relatorio

    # ------------------------------------------------------------------
    def _processar_numericos(self, coluna: pd.Series) -> pd.Series:
        if coluna.empty:
            return pd.to_numeric(coluna, errors='coerce')

        def _coerce_number(valor):
            if pd.isna(valor):
                return np.nan

            if isinstance(valor, (int, float, np.number)) and not isinstance(valor, bool):
                return valor

            texto = str(valor).strip()
            if not texto or texto.lower() in ('nan', 'none', 'null'):
                return np.nan

            texto = (
                texto.replace('\xa0', '')
                .replace('R$', '')
                .replace('%', '')
                .replace(' ', '')
            )

            if ',' in texto and '.' in texto:
                if texto.rfind(',') > texto.rfind('.'):
                    texto = texto.replace('.', '').replace(',', '.')
                else:
                    texto = texto.replace(',', '')
            elif ',' in texto:
                texto = texto.replace('.', '').replace(',', '.')

            return pd.to_numeric(texto, errors='coerce')

        return coluna.apply(_coerce_number)

    # ------------------------------------------------------------------
    def _processar_datas(self, coluna: pd.Series) -> pd.Series:
        datas = pd.to_datetime(coluna, dayfirst=True, errors='coerce')
        numericos = pd.to_numeric(coluna, errors='coerce')
        mascara = datas.isna() & numericos.notna()
        if mascara.any():
            datas.loc[mascara] = pd.to_datetime(
                numericos[mascara], unit='D', origin='1899-12-30', errors='coerce'
            )
        return datas
