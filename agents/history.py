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
        dias_rolling          = int(THRESHOLDS.get('dias_historico_rolling', 90))
        dias_rolling_fallback = int(THRESHOLDS.get('dias_historico_rolling_fallback', 180))

        if 'placa' not in df.columns or 'litros' not in df.columns:
            return df, novas

        # ────────────────────────────────────────────────────────────────
        # Preparar fonte de dados
        # ────────────────────────────────────────────────────────────────
        df_base_stats = df_historico if df_historico is not None else df
        df_base_stats = df_base_stats.copy()

        if 'placa' not in df_base_stats.columns or 'litros' not in df_base_stats.columns:
            return df, novas

        # Garantir coluna data_hora como datetime
        tem_data = 'data_hora' in df_base_stats.columns
        if tem_data:
            df_base_stats['data_hora'] = pd.to_datetime(df_base_stats['data_hora'], errors='coerce')
            data_max = df_base_stats['data_hora'].max()
        else:
            data_max = pd.NaT

        # Garantir coluna consumo
        if 'consumo' not in df_base_stats.columns:
            if 'km_rodado' in df_base_stats.columns and 'litros' in df_base_stats.columns:
                df_base_stats['consumo'] = df_base_stats['km_rodado'] / df_base_stats['litros'].replace(0, np.nan)
            else:
                df_base_stats['consumo'] = np.nan

        consumo_num = pd.to_numeric(df_base_stats['consumo'], errors='coerce')
        mask_valido = consumo_num.notna() & (consumo_num > 0) & (consumo_num <= consumo_max_valido)
        if '_erro_km' in df_base_stats.columns:
            mask_valido &= ~df_base_stats['_erro_km'].astype(bool)

        # ────────────────────────────────────────────────────────────────
        # Estatísticas adaptativas por placa (janela 90 dias → 180 dias)
        # ────────────────────────────────────────────────────────────────
        def _filtrar_janela(dias: int) -> pd.DataFrame:
            """Retorna df_base_stats filtrado para os últimos N dias."""
            if tem_data and pd.notna(data_max):
                return df_base_stats[df_base_stats['data_hora'] >= data_max - pd.Timedelta(days=dias)]
            return df_base_stats

        def _stats_placa(subdf: pd.DataFrame, mask: "pd.Series") -> dict:  # type: ignore[name-defined]
            sub = subdf.loc[mask & subdf.index.isin(subdf.index)]
            lit = sub['litros']
            cons = pd.to_numeric(sub['consumo'], errors='coerce')
            cons = cons[(cons > 0) & (cons <= consumo_max_valido)]
            return {
                'media_litros':   lit.mean(),
                'desvio_litros':  lit.std(),
                'media_consumo':  cons.mean(),
                'desvio_consumo': cons.std(),
                'mediana_litros': lit.median(),
                'mad_litros':     (lit - lit.median()).abs().median(),
                'mediana_consumo': cons.median(),
                'mad_consumo':    (cons - cons.median()).abs().median(),
                'contagem':       len(cons.dropna()),
            }

        df_90  = _filtrar_janela(dias_rolling)
        df_180 = _filtrar_janela(dias_rolling_fallback) if dias_rolling_fallback != dias_rolling else df_90

        # Consumo válido nos dois recortes
        cv_90  = pd.to_numeric(df_90['consumo'],  errors='coerce')
        cv_180 = pd.to_numeric(df_180['consumo'], errors='coerce')
        mask_90  = cv_90.notna()  & (cv_90 > 0)  & (cv_90 <= consumo_max_valido)
        mask_180 = cv_180.notna() & (cv_180 > 0) & (cv_180 <= consumo_max_valido)
        if '_erro_km' in df_90.columns:
            mask_90  &= ~df_90['_erro_km'].astype(bool)
        if '_erro_km' in df_180.columns:
            mask_180 &= ~df_180['_erro_km'].astype(bool)

        todas_placas = df_base_stats['placa'].dropna().unique()
        rows_stats = []
        for placa in todas_placas:
            # Tenta janela curta primeiro
            sub90_mask  = mask_90  & (df_90['placa']  == placa)
            cnt_90 = int(sub90_mask.sum())
            if cnt_90 >= min_hist:
                sub  = df_90[sub90_mask]
                dias_usados = dias_rolling
            else:
                sub180_mask = mask_180 & (df_180['placa'] == placa)
                sub  = df_180[sub180_mask]
                dias_usados = dias_rolling_fallback

            lit  = sub['litros']
            cons = pd.to_numeric(sub['consumo'], errors='coerce')
            cons_v = cons[(cons > 0) & (cons <= consumo_max_valido)]
            cnt = int(cons_v.dropna().__len__())

            # Contagem total na janela usada (para R11)
            cnt_total_mask = (df_180['placa'] == placa) if dias_usados == dias_rolling_fallback \
                             else (df_90['placa'] == placa)
            cnt_total = int(cnt_total_mask.sum())

            rows_stats.append({
                'placa':          placa,
                'media_litros':   lit.mean()   if cnt > 0 else np.nan,
                'desvio_litros':  lit.std()    if cnt > 0 else np.nan,
                'media_consumo':  cons_v.mean()  if cnt > 0 else np.nan,
                'desvio_consumo': cons_v.std()   if cnt > 0 else np.nan,
                'mediana_litros': lit.median() if cnt > 0 else np.nan,
                'mad_litros':     (lit - lit.median()).abs().median() if cnt > 0 else np.nan,
                'mediana_consumo': cons_v.median() if cnt > 0 else np.nan,
                'mad_consumo':    (cons_v - cons_v.median()).abs().median() if cnt > 0 else np.nan,
                'contagem':       cnt,
                'contagem_total': cnt_total,
                'janela_dias':    dias_usados,
            })

        stats = pd.DataFrame(rows_stats) if rows_stats else pd.DataFrame(
            columns=['placa','media_litros','desvio_litros','media_consumo','desvio_consumo',
                     'mediana_litros','mad_litros','mediana_consumo','mad_consumo',
                     'contagem','contagem_total','janela_dias']
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

        # medianas/MAD já calculados no loop adaptativo acima e mesclados via stats

        # ----------------------------------------------------------------
        # R09 — consumo km/L abaixo do padrão histórico da placa
        # ----------------------------------------------------------------
        novas += self._r09_anomalia_historica(df, min_hist)

        # ----------------------------------------------------------------
        # R10 — Km rodado muito abaixo do esperado pelo histórico
        # ----------------------------------------------------------------
        novas += self._r10_km_vs_esperado(df, min_hist)

        return df, novas

    # ------------------------------------------------------------------
    def _r09_anomalia_historica(self, df: pd.DataFrame, min_hist: int) -> list:
        """R09 — Consumo km/L real muito abaixo do padrão histórico da placa."""
        usar_mad    = bool(THRESHOLDS.get('usar_mad_outlier', True))
        fator_mad   = float(THRESHOLDS.get('fator_mad_historico', 3.0))
        fator_sigma = float(THRESHOLDS['fator_desvio_historico'])

        if 'consumo' not in df.columns or 'contagem' not in df.columns:
            return []

        consumo_num  = pd.to_numeric(df['consumo'],  errors='coerce')
        contagem_num = pd.to_numeric(df['contagem'], errors='coerce')
        mask = (
            contagem_num.notna() & (contagem_num >= min_hist)
            & consumo_num.notna() & (consumo_num > 0)
        )
        if '_erro_km' in df.columns:
            mask &= ~df['_erro_km'].astype(bool)
        df2 = df[mask].copy()
        if df2.empty:
            return []

        def _process(row):
            consumo   = row.get('consumo')
            contagem  = row.get('contagem')
            mediana_c = row.get('mediana_consumo')
            mad_c     = row.get('mad_consumo')
            media_c   = row.get('media_consumo')
            desvio_c  = row.get('desvio_consumo')
            usa_mad_aqui = (
                usar_mad and pd.notna(mediana_c) and pd.notna(mad_c) and mad_c > 0
            )
            if usa_mad_aqui:
                escala = 1.4826 * mad_c
                limite = mediana_c - fator_mad * escala
                if consumo >= limite:
                    return None
                janela = int(row.get('janela_dias', 90))
                return _oc(
                    'R09', 'consumo km/L abaixo do historico da placa',
                    (f"Consumo ({consumo:.2f} km/L) esta abaixo do limiar robusto "
                     f"({limite:.2f} km/L) da placa. "
                     f"Possivel desvio de combustivel ou falha mecanica."),
                    'MEDIA', row,
                    f"{consumo:.2f} km/L",
                    f">= {limite:.2f} km/L (mediana {mediana_c:.2f} - {fator_mad:.0f}×MAD {mad_c:.2f})",
                    (f"Base historica: {int(contagem)} registros ({janela} dias) | "
                     f"Mediana: {mediana_c:.2f} km/L | MAD: {mad_c:.2f} | "
                     f"Limite inferior MAD: {limite:.2f} km/L | Observado: {consumo:.2f} km/L."),
                    "Verificar se ha falha mecanica ou desvio de combustivel que explique consumo "
                    "muito abaixo do padrao historico deste veiculo.",
                )
            # Fallback sigma
            if any(pd.isna(v) for v in [media_c, desvio_c]) or (desvio_c or 0) == 0:
                return None
            limite = media_c - fator_sigma * desvio_c
            if consumo >= limite:
                return None
            janela = int(row.get('janela_dias', 90))
            return _oc(
                'R09', 'consumo km/L abaixo do historico da placa',
                (f"Consumo ({consumo:.2f} km/L) esta mais de {fator_sigma:.0f} desvios "
                 f"abaixo da media historica da placa ({media_c:.2f} km/L). "
                 f"Possivel desvio de combustivel ou falha mecanica."),
                'MEDIA', row,
                f"{consumo:.2f} km/L",
                f">= {limite:.2f} km/L (media {media_c:.2f} - {fator_sigma:.0f}σ {desvio_c:.2f})",
                (f"Base historica: {int(contagem)} registros ({janela} dias) | "
                 f"Media: {media_c:.2f} km/L | Desvio: {desvio_c:.2f} | "
                 f"Limite: {limite:.2f} km/L | Observado: {consumo:.2f} km/L."),
                "Verificar se ha falha mecanica ou desvio de combustivel que explique consumo "
                "muito abaixo do padrao historico deste veiculo.",
            )

        return [r for r in df2.apply(_process, axis=1) if r is not None]

    # ------------------------------------------------------------------
    def _r10_km_vs_esperado(self, df: pd.DataFrame, min_hist: int) -> list:
        razao_min = THRESHOLDS.get('razao_km_rodado_esperado_min', 60.0)
        if 'km_rodado' not in df.columns or 'km_esperado' not in df.columns:
            return []
        need = ['km_rodado', 'km_esperado', 'contagem']
        df2 = df[df[need].notna().all(axis=1)].copy()
        df2 = df2[
            (pd.to_numeric(df2['contagem'], errors='coerce') >= min_hist)
            & (df2['km_esperado'] > 0)
        ]
        df2['_razao'] = (df2['km_rodado'] / df2['km_esperado']) * 100
        df2 = df2[df2['_razao'] < razao_min]
        if df2.empty:
            return []
        def _make(row):
            km, esp, razao = row['km_rodado'], row['km_esperado'], row['_razao']
            janela = int(row.get('janela_dias', 90))
            return _oc(
                'R10', 'km rodado muito abaixo do esperado pelo historico',
                (f"Veiculo rodou {razao:.1f}% do km esperado "
                 f"({km:.0f} km rodados de {esp:.0f} km esperados), "
                 f"com base na media historica de consumo da placa e nos litros abastecidos."),
                'MEDIA', row,
                f"{km:.0f} km ({razao:.1f}% do esperado)",
                f">= {razao_min:.0f}% de {esp:.0f} km (litros × media_consumo_placa)",
                (f"Litros: {row.get('litros', '?'):.2f} L | "
                 f"Media consumo placa: {row.get('media_consumo', '?'):.2f} km/L | "
                 f"Km esperado: {esp:.0f} km | "
                 f"Km rodado: {km:.0f} km | "
                 f"Realizou {razao:.1f}% do esperado (minimo: {razao_min:.0f}%) | "
                 f"Janela historica: {janela} dias."),
                "Verificar consistencia entre quilometragem declarada e utilizacao efetiva do veiculo.",
            )
        return df2.apply(_make, axis=1).tolist()
