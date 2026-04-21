import numpy as np
import pandas as pd

from .config import THRESHOLDS
from .rules import _oc


class AgentHistorico:
    """
    Agente 4 — Contexto Histórico.
    Calcula médias e desvios por placa, enriquece o DataFrame
    e gera alertas R09 (anomalia histórica de litros) e R10 (km abaixo do esperado).
    """

    def processar(self, df: pd.DataFrame, ocorrencias: list) -> tuple:
        """
        Retorna (df_enriquecido, ocorrencias_atualizadas).
        """
        df = df.copy()
        novas = list(ocorrencias)
        min_hist = THRESHOLDS['minimo_historico_para_comparacao']

        if 'placa' not in df.columns or 'litros' not in df.columns:
            return df, novas

        # ----------------------------------------------------------------
        # Estatísticas históricas por placa
        # ----------------------------------------------------------------
        stats = (
            df.groupby('placa')
            .agg(
                media_litros   = ('litros',   'mean'),
                desvio_litros  = ('litros',   'std'),
                media_consumo  = ('consumo',  'mean'),
                desvio_consumo = ('consumo',  'std'),
                contagem       = ('litros',   'count'),
            )
            .reset_index()
        )

        df = df.merge(stats, on='placa', how='left')

        # ----------------------------------------------------------------
        # km esperado = litros abastecidos × média histórica de consumo da placa
        # ----------------------------------------------------------------
        df['km_esperado'] = df['litros'] * df['media_consumo']
        if 'km_rodado' in df.columns:
            df['desvio_km_esperado'] = df['km_rodado'] - df['km_esperado']
            df['desvio_km_esperado_pct'] = np.where(
                df['km_esperado'].fillna(0) > 0,
                (df['desvio_km_esperado'] / df['km_esperado']) * 100,
                np.nan,
            )
        else:
            df['desvio_km_esperado'] = np.nan
            df['desvio_km_esperado_pct'] = np.nan

        # ----------------------------------------------------------------
        # R09 — Litros muito acima do padrão histórico da placa
        # ----------------------------------------------------------------
        novas += self._r09_anomalia_historica(df, min_hist)

        # ----------------------------------------------------------------
        # R10 — Km rodado muito abaixo do esperado pelo histórico
        # ----------------------------------------------------------------
        novas += self._r10_km_vs_esperado(df, min_hist)

        return df, novas

    # ------------------------------------------------------------------
    def _r09_anomalia_historica(self, df: pd.DataFrame, min_hist: int) -> list:
        res = []
        fator = THRESHOLDS['fator_desvio_historico']
        for _, row in df.iterrows():
            contagem = row.get('contagem', 0)
            if pd.isna(contagem) or contagem < min_hist:
                continue
            media  = row.get('media_litros')
            desvio = row.get('desvio_litros')
            litros = row.get('litros')
            if any(pd.isna(v) for v in [media, desvio, litros]) or desvio == 0:
                continue
            limite = media + fator * desvio
            if litros > limite:
                res.append(_oc(
                    'R09',
                    'anomalia financeira por historico de litros',
                    (f"Volume abastecido ({litros:.2f} L) supera em mais de "
                     f"{fator:.0f} desvios padrão a média histórica da placa "
                     f"({media:.2f} L). Possivel anomalia financeira."),
                    'MEDIA', row,
                    f"{litros:.2f} L",
                    f"<= {limite:.2f} L (media {media:.2f} + {fator:.0f}σ {desvio:.2f})",
                    (f"Base historica: {int(contagem)} registros | "
                     f"Media: {media:.2f} L | Desvio: {desvio:.2f} L | "
                     f"Limite: {limite:.2f} L | Observado: {litros:.2f} L."),
                    "Verificar necessidade operacional que justifique volume muito acima do historico da placa.",
                ))
        return res

    # ------------------------------------------------------------------
    def _r10_km_vs_esperado(self, df: pd.DataFrame, min_hist: int) -> list:
        res = []
        pct_lim = THRESHOLDS['desvio_km_esperado_pct']
        if 'km_rodado' not in df.columns or 'km_esperado' not in df.columns:
            return res
        for _, row in df.iterrows():
            contagem = row.get('contagem', 0)
            if pd.isna(contagem) or contagem < min_hist:
                continue
            km       = row.get('km_rodado')
            esperado = row.get('km_esperado')
            if any(pd.isna(v) for v in [km, esperado]) or esperado <= 0:
                continue
            pct = ((km - esperado) / esperado) * 100
            if pct < -pct_lim:
                res.append(_oc(
                    'R10',
                    'km rodado muito abaixo do esperado pelo historico',
                    (f"Km rodado ({km:.0f} km) esta {abs(pct):.1f}% abaixo do "
                     f"esperado ({esperado:.0f} km) com base na media "
                     f"historica de consumo da placa e nos litros abastecidos."),
                    'MEDIA', row,
                    f"{km:.0f} km",
                    f"{esperado:.0f} km (litros × media_consumo_placa)",
                    (f"Litros: {row.get('litros', '?'):.2f} L | "
                     f"Media consumo placa: {row.get('media_consumo', '?'):.2f} km/L | "
                     f"Km esperado: {esperado:.0f} km | "
                     f"Km rodado: {km:.0f} km | "
                     f"Desvio: {pct:.1f}%."),
                    "Verificar consistencia entre quilometragem declarada e utilizacao efetiva do veiculo.",
                ))
        return res
