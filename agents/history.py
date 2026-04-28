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

    def processar(self, df: pd.DataFrame, ocorrencias: list, df_historico: pd.DataFrame = None) -> tuple:
        """
        Retorna (df_enriquecido, ocorrencias_atualizadas).
        df_historico: DataFrame com histórico completo para cálculo de médias (últimos 90 dias).
                      Se None, usa apenas df (recorte filtrado).
        """
        df = df.copy()
        novas = list(ocorrencias)
        min_hist = THRESHOLDS['minimo_historico_para_comparacao']
        consumo_max_valido = float(THRESHOLDS.get('consumo_max_valido', 100.0))
        dias_rolling = int(THRESHOLDS.get('dias_historico_rolling', 90))

        if 'placa' not in df.columns or 'litros' not in df.columns:
            return df, novas

        # ────────────────────────────────────────────────────────────────
        # Preparar fonte de dados para calcular médias históricas
        # ────────────────────────────────────────────────────────────────
        df_base_stats = df_historico if df_historico is not None else df
        df_base_stats = df_base_stats.copy()

        # Validar que as colunas necessárias existem em df_base_stats
        if 'placa' not in df_base_stats.columns or 'litros' not in df_base_stats.columns:
            return df, novas

        # Filtrar últimos N dias se houver coluna data_hora
        if 'data_hora' in df_base_stats.columns:
            df_base_stats['data_hora'] = pd.to_datetime(df_base_stats['data_hora'], errors='coerce')
            data_max = df_base_stats['data_hora'].max()
            if pd.notna(data_max):
                data_min = data_max - pd.Timedelta(days=dias_rolling)
                df_base_stats = df_base_stats[df_base_stats['data_hora'] >= data_min]

        # ────────────────────────────────────────────────────────────────
        # Calcular estatísticas históricas por placa
        # ────────────────────────────────────────────────────────────────
        # Se consumo não existe, calcular a partir de km_rodado/litros
        if 'consumo' not in df_base_stats.columns:
            if 'km_rodado' in df_base_stats.columns and 'litros' in df_base_stats.columns:
                df_base_stats['consumo'] = df_base_stats['km_rodado'] / df_base_stats['litros'].replace(0, np.nan)
            else:
                df_base_stats['consumo'] = np.nan
        
        consumo_valido = pd.to_numeric(df_base_stats['consumo'], errors='coerce')
        mask_stats = consumo_valido.notna() & (consumo_valido > 0) & (consumo_valido <= consumo_max_valido)
        if '_erro_km' in df_base_stats.columns:
            mask_stats &= ~df_base_stats['_erro_km'].astype(bool)

        df_stats = df_base_stats.loc[mask_stats, ['placa', 'litros']].copy()
        df_stats['consumo'] = consumo_valido.loc[mask_stats]

        # Contagem TOTAL de registros por placa na janela (antes do filtro de validade)
        # Usada para dar mensagem precisa no R11: distingue "sem histórico" de "histórico inválido"
        contagem_total_series = df_base_stats.groupby('placa')['litros'].count().reset_index()
        contagem_total_series.columns = ['placa', 'contagem_total']

        stats = (
            df_stats.groupby('placa')
            .agg(
                media_litros   = ('litros',   'mean'),
                desvio_litros  = ('litros',   'std'),
                media_consumo  = ('consumo',  'mean'),
                desvio_consumo = ('consumo',  'std'),
                contagem       = ('litros',   'count'),
            )
            .reset_index()
        )
        stats = stats.merge(contagem_total_series, on='placa', how='outer')

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

        # Adicionar coluna 'mediana_litros' e 'mad_litros' para R09 robusto
        if 'placa' in df_stats.columns and 'litros' in df_stats.columns:
            mad_stats = (
                df_stats.groupby('placa')
                .apply(lambda g: pd.Series({
                    'mediana_litros': g['litros'].median(),
                    'mad_litros':     (g['litros'] - g['litros'].median()).abs().median(),
                }))
                .reset_index()
            )
            df = df.merge(mad_stats, on='placa', how='left')
        else:
            df['mediana_litros'] = np.nan
            df['mad_litros'] = np.nan

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
        usar_mad = bool(THRESHOLDS.get('usar_mad_outlier', True))
        fator_mad = float(THRESHOLDS.get('fator_mad_historico', 3.0))
        fator_sigma = float(THRESHOLDS['fator_desvio_historico'])

        for _, row in df.iterrows():
            contagem = row.get('contagem', 0)
            if pd.isna(contagem) or contagem < min_hist:
                continue
            litros   = row.get('litros')
            mediana  = row.get('mediana_litros')
            mad      = row.get('mad_litros')
            media    = row.get('media_litros')
            desvio   = row.get('desvio_litros')
            if pd.isna(litros):
                continue

            usa_mad_aqui = (
                usar_mad
                and pd.notna(mediana)
                and pd.notna(mad)
                and mad > 0
            )

            if usa_mad_aqui:
                escala = 1.4826 * mad
                limite = mediana + fator_mad * escala
                if litros > limite:
                    res.append(_oc(
                        'R09',
                        'anomalia financeira por historico de litros',
                        (f"Volume abastecido ({litros:.2f} L) supera o limiar robusto "
                         f"({limite:.2f} L) da placa. Possivel anomalia financeira."),
                        'MEDIA', row,
                        f"{litros:.2f} L",
                        f"<= {limite:.2f} L (mediana {mediana:.2f} + {fator_mad:.0f}×MAD {mad:.2f})",
                        (f"Base historica: {int(contagem)} registros | "
                         f"Mediana: {mediana:.2f} L | MAD: {mad:.2f} L | "
                         f"Limite MAD: {limite:.2f} L | Observado: {litros:.2f} L."),
                        "Verificar necessidade operacional que justifique volume muito acima do historico da placa.",
                    ))
            else:
                if any(pd.isna(v) for v in [media, desvio]) or (desvio or 0) == 0:
                    continue
                limite = media + fator_sigma * desvio
                if litros > limite:
                    res.append(_oc(
                        'R09',
                        'anomalia financeira por historico de litros',
                        (f"Volume abastecido ({litros:.2f} L) supera em mais de "
                         f"{fator_sigma:.0f} desvios padrao a media historica da placa "
                         f"({media:.2f} L). Possivel anomalia financeira."),
                        'MEDIA', row,
                        f"{litros:.2f} L",
                        f"<= {limite:.2f} L (media {media:.2f} + {fator_sigma:.0f}σ {desvio:.2f})",
                        (f"Base historica: {int(contagem)} registros | "
                         f"Media: {media:.2f} L | Desvio: {desvio:.2f} L | "
                         f"Limite: {limite:.2f} L | Observado: {litros:.2f} L."),
                        "Verificar necessidade operacional que justifique volume muito acima do historico da placa.",
                    ))
        return res

    # ------------------------------------------------------------------
    def _r10_km_vs_esperado(self, df: pd.DataFrame, min_hist: int) -> list:
        res = []
        razao_min = THRESHOLDS.get('razao_km_rodado_esperado_min', 60.0)
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
            razao = (km / esperado) * 100
            if razao < razao_min:
                res.append(_oc(
                    'R10',
                    'km rodado muito abaixo do esperado pelo historico',
                    (f"Veiculo rodou {razao:.1f}% do km esperado "
                     f"({km:.0f} km rodados de {esperado:.0f} km esperados), "
                     f"com base na media historica de consumo da placa e nos litros abastecidos."),
                    'MEDIA', row,
                    f"{km:.0f} km ({razao:.1f}% do esperado)",
                    f">= {razao_min:.0f}% de {esperado:.0f} km (litros × media_consumo_placa)",
                    (f"Litros: {row.get('litros', '?'):.2f} L | "
                     f"Media consumo placa: {row.get('media_consumo', '?'):.2f} km/L | "
                     f"Km esperado: {esperado:.0f} km | "
                     f"Km rodado: {km:.0f} km | "
                     f"Realizou {razao:.1f}% do esperado (minimo: {razao_min:.0f}%)."),
                    "Verificar consistencia entre quilometragem declarada e utilizacao efetiva do veiculo.",
                ))
        return res
