import numpy as np
import pandas as pd

from .config import TANK_CAPACITY, THRESHOLDS


# ---------------------------------------------------------------------------
# Helper: monta um dicionário de ocorrência padronizado
# ---------------------------------------------------------------------------
def _oc(codigo, tipo, descricao, gravidade, row,
        valor_observado, valor_referencia, evidencia, recomendacao):
    data = row.get('data_hora')
    litros = row.get('litros')
    media_referencia = row.get('media_consumo_modelo')
    if pd.isna(media_referencia):
        media_referencia = row.get('media_consumo')

    desvio_referencia = row.get('desvio_consumo_modelo')
    if pd.isna(desvio_referencia):
        desvio_referencia = row.get('desvio_consumo')

    km_esperado = row.get('km_esperado', pd.NA)
    if pd.isna(km_esperado) and pd.notna(litros) and pd.notna(media_referencia):
        km_esperado = litros * media_referencia

    return {
        'codigo_regra':    codigo,
        'placa':           row.get('placa', ''),
        'condutor':        row.get('condutor', ''),
        'modelo':          row.get('modelo', ''),
        'unidade':         row.get('unidade', ''),
        'data_hora':       data,
        'km_anterior':     row.get('ult_km', pd.NA),
        'km_atual':        row.get('km_atual', pd.NA),
        'km_rodados':      row.get('km_rodado', pd.NA),
        'km_esperado':     km_esperado,
        'km_l':            row.get('consumo', pd.NA),
        'litros':          litros if pd.notna(litros) else pd.NA,
        'media':           media_referencia if pd.notna(media_referencia) else pd.NA,
        'min':             row.get('km_minimo', pd.NA),
        'max':             row.get('km_maximo', pd.NA),
        'desvio':          desvio_referencia if pd.notna(desvio_referencia) else pd.NA,
        'estabelecimento': row.get('estabelecimento', ''),
        'produto':         row.get('produto', ''),
        'tipo_ocorrencia': tipo,
        'descricao_tecnica':  descricao,
        'gravidade_inicial':  gravidade,
        'valor_observado':    str(valor_observado),
        'valor_referencia':   str(valor_referencia),
        'evidencia':          evidencia,
        'recomendacao':       recomendacao,
    }


def _ok(row, *cols):
    """Verifica se todas as colunas existem e têm valor não-nulo na linha."""
    return all(c in row.index and pd.notna(row[c]) for c in cols)


# ---------------------------------------------------------------------------
class AgentRegras:
    """
    Agente 3 — Motor de Regras Operacionais.
    Aplica as 9 regras de auditoria determinísticas e retorna lista de ocorrências.
    """

    def processar(self, df: pd.DataFrame, params: dict = None) -> list:
        params = params or {}
        ocs = []

        # Registros com _erro_km=True têm km inválido (erro de digitação).
        # São excluídos das regras que dependem de quilometragem/consumo,
        # mas permanecem na base para exibição no relatório de qualidade.
        df_km_ok = df[~df['_erro_km'].astype(bool)].copy() if '_erro_km' in df.columns else df

        # df_historico_modelo: base de 90 dias para calcular estatísticas por modelo no R03.
        # Passado via params['df_historico_modelo'] pelo orchestrator.
        df_hist_modelo = params.get('df_historico_modelo')

        ocs += self._r01_capacidade_tanque(df)          # litros — usa df completo
        ocs += self._r00_consumo_impossivel(df)         # km/L absurdo — usa df completo antes de filtrar
        ocs += self._r02_consumo_fora_faixa(df_km_ok)   # consumo depende de km
        ocs += self._r03_consumo_critico(
            df_km_ok,
            sigma_override=params.get('outlier_sigma_mult'),
            df_historico=df_hist_modelo,
        )
        ocs += self._r04_hodometro(df_km_ok)
        ocs += self._r05_km_incompativel(df_km_ok)
        ocs += self._r06_abastecimentos_proximos(df)    # intervalo de tempo — ok usar df completo
        ocs += self._r07_preco_acima_contratado(df)     # preço — independe de km
        ocs += self._r08_valor_inconsistente(df)        # valor — independe de km
        ocs += self._r11_historico_insuficiente(df)     # aviso para placas sem histórico
        # R09 e R10 são gerados pelo AgentHistorico com dados de contexto
        return ocs

    # ------------------------------------------------------------------
    # R00 — Consumo km/L fisicamente impossível (hodômetro inválido)
    # ------------------------------------------------------------------
    def _r00_consumo_impossivel(self, df: pd.DataFrame) -> list:
        res = []
        consumo_max = float(THRESHOLDS.get('consumo_max_valido', 30.0))
        if 'consumo' not in df.columns:
            return res
        df_alto = df[pd.to_numeric(df['consumo'], errors='coerce') > consumo_max].copy()
        for _, row in df_alto.iterrows():
            consumo_val = pd.to_numeric(row.get('consumo'), errors='coerce')
            if pd.isna(consumo_val):
                continue
            res.append(_oc(
                codigo='R00',
                tipo='HODOMETRO_INVALIDO_CONSUMO_IMPOSSIVEL',
                descricao=(
                    f"Consumo de {consumo_val:.1f} km/L é fisicamente impossível para veículos terrestres "
                    f"(limite: {consumo_max:.0f} km/L). Provável erro de leitura de hodômetro."
                ),
                gravidade='ALTA',
                row=row,
                valor_observado=f"{consumo_val:.1f} km/L",
                valor_referencia=f"≤ {consumo_max:.0f} km/L",
                evidencia=(
                    f"km_rodado={row.get('km_rodado', 'N/A')} | "
                    f"litros={row.get('litros', 'N/A')} | "
                    f"consumo calculado={consumo_val:.1f} km/L"
                ),
                recomendacao=(
                    "Verificar leitura do hodômetro na data do abastecimento. "
                    "Solicitar conferência física do km_atual informado pelo motorista."
                ),
            ))
        return res

    # ------------------------------------------------------------------
    # R01 — Capacidade do tanque
    # ------------------------------------------------------------------
    def _r01_capacidade_tanque(self, df: pd.DataFrame) -> list:
        res = []
        fator = THRESHOLDS['fator_tolerancia_tanque']
        if 'litros' not in df.columns or 'modelo_norm' not in df.columns:
            return res
        for _, row in df.iterrows():
            if not _ok(row, 'litros', 'modelo_norm'):
                continue
            mn = row['modelo_norm']
            capacidade = next((v for k, v in TANK_CAPACITY.items() if k in mn), None)
            if capacidade is None:
                continue
            limite = capacidade * fator
            if row['litros'] >= limite:
                res.append(_oc(
                    'R01',
                    'abastecimento acima da capacidade estimada do tanque',
                    (f"Volume abastecido ({row['litros']:.2f} L) esta igual ou acima de {fator*100:.0f}% "
                     f"da capacidade estimada do tanque ({capacidade} L) "
                     f"para o modelo {row.get('modelo', mn)}."),
                    'ALTA', row,
                    f"{row['litros']:.2f} L",
                    f"< {limite:.2f} L ({capacidade} L x {fator})",
                    (f"Capacidade cadastrada: {capacidade} L | "
                     f"Litros abastecidos: {row['litros']:.2f} L | "
                     f"Excesso: {row['litros'] - capacidade:.2f} L."),
                    "Confrontar com cupom fiscal e verificar se houve mais de um acionamento de bomba.",
                ))
        return res

    # ------------------------------------------------------------------
    # R02 — Consumo fora da faixa [KM Minimo, KM Maximo]
    # ------------------------------------------------------------------
    def _r02_consumo_fora_faixa(self, df: pd.DataFrame) -> list:
        res = []
        if 'consumo' not in df.columns:
            return res
        for _, row in df.iterrows():
            if not _ok(row, 'consumo', 'km_minimo', 'km_maximo'):
                continue
            c, km_min, km_max = row['consumo'], row['km_minimo'], row['km_maximo']
            if km_min <= 0 or km_max <= 0:
                continue
            if c < km_min or c > km_max:
                direcao = 'abaixo do minimo' if c < km_min else 'acima do maximo'
                ref = km_min if c < km_min else km_max
                res.append(_oc(
                    'R02',
                    'desvio de consumo fora da faixa esperada',
                    (f"Consumo apurado ({c:.2f} km/L) esta {direcao} "
                     f"da faixa esperada [{km_min:.1f} – {km_max:.1f}] km/L."),
                    'MEDIA', row,
                    f"{c:.2f} km/L",
                    f"[{km_min:.1f} – {km_max:.1f}] km/L",
                    (f"KM min: {km_min:.1f} | KM max: {km_max:.1f} | "
                     f"Consumo apurado: {c:.2f} km/L | "
                     f"Desvio: {abs(c - ref):.2f} km/L."),
                    "Verificar condicoes do veiculo, percurso e registro de km rodado.",
                ))
        return res

    # ------------------------------------------------------------------
    # R03 — Rendimento criticamente baixo por outlier estatistico
    # ------------------------------------------------------------------
    def _r03_consumo_critico(self, df: pd.DataFrame, sigma_override=None, df_historico=None) -> list:
        res = []
        if 'consumo' not in df.columns:
            return res

        n_sigma = THRESHOLDS.get('fator_outlier_consumo_critico', 2.0)
        if sigma_override is not None:
            try:
                n_sigma = float(sigma_override)
            except (TypeError, ValueError):
                pass
        min_amostra = int(THRESHOLDS.get('min_amostra_outlier_consumo', 5))
        fator_fallback = THRESHOLDS.get('fator_consumo_critico', 0.50)
        consumo_max_valido = float(THRESHOLDS.get('consumo_max_valido', 100.0))

        # Estatísticas por modelo calculadas sobre o histórico de 90 dias (se disponível),
        # não apenas sobre o recorte filtrado — evita viés de seleção de período.
        df_stats_src = df_historico if df_historico is not None else df

        if 'modelo_norm' in df_stats_src.columns and 'consumo' in df_stats_src.columns:
            consumo_hist = pd.to_numeric(df_stats_src['consumo'], errors='coerce')
            mask_val = consumo_hist.notna() & (consumo_hist > 0) & (consumo_hist <= consumo_max_valido)
            if '_erro_km' in df_stats_src.columns:
                mask_val &= ~df_stats_src['_erro_km'].astype(bool)
            df_src_clean = df_stats_src.loc[mask_val, ['modelo_norm', 'consumo']].copy()
            df_src_clean['consumo'] = consumo_hist.loc[mask_val]

            def _stats_modelo(grp):
                c = grp['consumo']
                med = c.median()
                mad = (c - med).abs().median()
                return pd.Series({
                    'media_consumo_modelo':  c.mean(),
                    'mediana_consumo_modelo': med,
                    'mad_consumo_modelo':    mad,
                    'desvio_consumo_modelo': c.std(),
                    'qtd_modelo':            len(c),
                })

            stats = df_src_clean.groupby('modelo_norm', dropna=False).apply(_stats_modelo).reset_index()
            df_ref = df.merge(stats, on='modelo_norm', how='left')
        else:
            df_ref = df.copy()
            for col in ('media_consumo_modelo', 'mediana_consumo_modelo', 'mad_consumo_modelo',
                        'desvio_consumo_modelo', 'qtd_modelo'):
                df_ref[col] = pd.NA

        usar_mad = bool(THRESHOLDS.get('usar_mad_outlier', True))

        for _, row in df_ref.iterrows():
            if not _ok(row, 'consumo'):
                continue

            c = row['consumo']

            qtd_modelo   = row.get('qtd_modelo', 0) or 0
            mediana_m    = row.get('mediana_consumo_modelo')
            mad_m        = row.get('mad_consumo_modelo')
            media_m      = row.get('media_consumo_modelo')
            desvio_m     = row.get('desvio_consumo_modelo')

            usa_mad_modelo = (
                usar_mad
                and pd.notna(mediana_m)
                and pd.notna(mad_m)
                and mad_m > 0
                and qtd_modelo >= min_amostra
            )
            usa_sigma_modelo = (
                not usa_mad_modelo
                and pd.notna(media_m)
                and pd.notna(desvio_m)
                and desvio_m > 0
                and qtd_modelo >= min_amostra
            )

            if usa_mad_modelo:
                # MAD robusto: insensível a outros outliers na amostra
                escala_mad = 1.4826 * mad_m
                limite = mediana_m - (n_sigma * escala_mad)
                condicao_critica = c < limite
                referencia = (
                    f">= {limite:.2f} km/L "
                    f"(mediana modelo {mediana_m:.2f} - {n_sigma:.1f}×1.4826×MAD {mad_m:.2f})"
                )
                evidencia = (
                    f"Modelo: {row.get('modelo', row.get('modelo_norm', ''))} | "
                    f"Base historica: {int(qtd_modelo)} registros | "
                    f"Mediana: {mediana_m:.2f} km/L | MAD: {mad_m:.2f} | "
                    f"Limiar MAD: {limite:.2f} km/L | "
                    f"Consumo apurado: {c:.2f} km/L."
                )
            elif usa_sigma_modelo:
                limite = media_m - (n_sigma * desvio_m)
                condicao_critica = c < limite
                referencia = (
                    f">= {limite:.2f} km/L "
                    f"(media modelo {media_m:.2f} - {n_sigma:.1f}σ {desvio_m:.2f})"
                )
                evidencia = (
                    f"Modelo: {row.get('modelo', row.get('modelo_norm', ''))} | "
                    f"Amostra: {int(qtd_modelo)} | "
                    f"Media modelo: {media_m:.2f} km/L | "
                    f"Desvio modelo: {desvio_m:.2f} km/L | "
                    f"Limiar outlier: {limite:.2f} km/L | "
                    f"Consumo apurado: {c:.2f} km/L."
                )
            else:
                # Fallback: requer km_minimo valido
                km_min = row.get('km_minimo')
                if not pd.notna(km_min) or float(km_min) <= 0:
                    continue
                km_min = float(km_min)
                limite = km_min * fator_fallback
                condicao_critica = c < limite
                referencia = f">= {limite:.2f} km/L ({fator_fallback*100:.0f}% de {km_min:.1f} km/L)"
                evidencia = (
                    f"Regra fallback por baixa amostra do modelo | "
                    f"KM minimo esperado: {km_min:.1f} km/L | "
                    f"Limiar fallback: {limite:.2f} km/L | "
                    f"Consumo apurado: {c:.2f} km/L."
                )

            if condicao_critica:
                res.append(_oc(
                    'R03',
                    'rendimento criticamente baixo',
                    (f"Rendimento apurado ({c:.2f} km/L) esta abaixo do limiar critico "
                     f"para o modelo, indicando possivel ineficiencia anomala."),
                    'ALTA', row,
                    f"{c:.2f} km/L",
                    referencia,
                    evidencia,
                    "Apurar causa: possivel desvio de combustivel, erro de odometro ou lancamento incorreto.",
                ))
        return res

    # ------------------------------------------------------------------
    # R04 — Inconsistência de hodômetro (km Atual < Ult. km)
    # ------------------------------------------------------------------
    def _r04_hodometro(self, df: pd.DataFrame) -> list:
        res = []
        if 'ult_km' not in df.columns or 'km_atual' not in df.columns:
            return res
        for _, row in df.iterrows():
            if not _ok(row, 'ult_km', 'km_atual'):
                continue
            if row['km_atual'] < row['ult_km']:
                diff = row['ult_km'] - row['km_atual']
                res.append(_oc(
                    'R04',
                    'inconsistencia de hodometro',
                    (f"km Atual ({row['km_atual']:.0f}) e inferior a Ult. km "
                     f"({row['ult_km']:.0f}), indicando inversao ou possivel "
                     f"manipulacao do hodometro."),
                    'ALTA', row,
                    f"km Atual: {row['km_atual']:.0f}",
                    f"Deve ser >= Ult. km: {row['ult_km']:.0f}",
                    (f"Ult. km: {row['ult_km']:.0f} | "
                     f"km Atual: {row['km_atual']:.0f} | "
                     f"Diferenca: -{diff:.0f} km."),
                    "Confrontar com documentos de transporte e rastreamento veicular.",
                ))
        return res

    # ------------------------------------------------------------------
    # R05 — Km rodado incompatível com litros abastecidos
    # ------------------------------------------------------------------
    def _r05_km_incompativel(self, df: pd.DataFrame) -> list:
        res = []
        km_lim = THRESHOLDS['km_baixo_limite']
        lit_lim = THRESHOLDS['litros_alto_limite']
        if 'km_rodado' not in df.columns or 'litros' not in df.columns:
            return res
        for _, row in df.iterrows():
            if not _ok(row, 'km_rodado', 'litros'):
                continue
            if row['km_rodado'] <= km_lim and row['litros'] >= lit_lim:
                res.append(_oc(
                    'R05',
                    'km rodado incompativel com volume abastecido',
                    (f"Km rodado ({row['km_rodado']:.0f} km) muito baixo para "
                     f"o volume abastecido ({row['litros']:.2f} L). "
                     f"Forte indicio de inconsistencia operacional."),
                    'ALTA', row,
                    f"{row['km_rodado']:.0f} km / {row['litros']:.2f} L",
                    f"Km rodado > {km_lim} km ou litros < {lit_lim} L",
                    (f"Km rodado: {row['km_rodado']:.0f} km | "
                     f"Litros: {row['litros']:.2f} L | "
                     f"Consumo implicito: {row['litros']/(max(row['km_rodado'],1)):.2f} L/km."),
                    "Confrontar com relatorio de trafego, rastreamento veicular e cupom fiscal.",
                ))
        return res

    # ------------------------------------------------------------------
    # R06 — Abastecimentos muito próximos (< 4 horas)
    # ------------------------------------------------------------------
    def _r06_abastecimentos_proximos(self, df: pd.DataFrame) -> list:
        res = []
        if 'placa' not in df.columns or 'data_hora' not in df.columns:
            return res
        limite_h = THRESHOLDS['limite_intervalo_horas']
        df_ord = df.sort_values(['placa', 'data_hora']).copy()
        df_ord['_delta_h'] = (
            df_ord.groupby('placa')['data_hora']
            .diff()
            .dt.total_seconds()
            .div(3600)
        )
        for _, row in df_ord[df_ord['_delta_h'].notna() & (df_ord['_delta_h'] < limite_h)].iterrows():
            dh = row['_delta_h']
            res.append(_oc(
                'R06',
                'recorrencia de abastecimento em intervalo muito curto',
                (f"Abastecimento realizado {dh:.1f} h apos o anterior "
                 f"para a mesma placa, abaixo do limite de {limite_h} h."),
                'MEDIA', row,
                f"{dh:.1f} h desde o anterior",
                f">= {limite_h} h",
                (f"Intervalo: {dh:.1f} h | "
                 f"Litros neste evento: {row.get('litros', '?'):.2f} L."),
                "Verificar necessidade operacional que justifique dois abastecimentos em curto periodo.",
            ))
        return res

    # ------------------------------------------------------------------
    # R07 — Preço unitário acima do contratado
    #        (só ativa se houver coluna 'preco_contratado' no DataFrame)
    # ------------------------------------------------------------------
    def _r07_preco_acima_contratado(self, df: pd.DataFrame) -> list:
        res = []
        if 'valor_unitario' not in df.columns or 'preco_contratado' not in df.columns:
            return res
        tol = THRESHOLDS['tolerancia_preco_unitario']
        for _, row in df.iterrows():
            if not _ok(row, 'valor_unitario', 'preco_contratado'):
                continue
            excesso = row['valor_unitario'] - row['preco_contratado']
            if excesso > tol:
                res.append(_oc(
                    'R07',
                    'divergencia de preco contratado',
                    (f"Preco unitario cobrado (R$ {row['valor_unitario']:.4f}/L) "
                     f"supera o preco contratado (R$ {row['preco_contratado']:.4f}/L) "
                     f"em R$ {excesso:.4f}/L."),
                    'MEDIA', row,
                    f"R$ {row['valor_unitario']:.4f}/L",
                    f"R$ {row['preco_contratado']:.4f}/L (+ tolerancia R$ {tol:.2f})",
                    (f"Preco cobrado: R$ {row['valor_unitario']:.4f}/L | "
                     f"Preco contratado: R$ {row['preco_contratado']:.4f}/L | "
                     f"Excesso: R$ {excesso:.4f}/L."),
                    "Verificar contrato vigente e acionar o setor de contratos para regularizacao.",
                ))
        return res

    # ------------------------------------------------------------------
    # R08 — Valor total inconsistente com Qtde x Vr. Unit.
    #        Tolerância percentual (2%) com piso absoluto (R$0,50)
    # ------------------------------------------------------------------
    def _r08_valor_inconsistente(self, df: pd.DataFrame) -> list:
        res = []
        tol_pct = THRESHOLDS.get('tolerancia_valor_total_pct', 0.02)
        tol_abs = THRESHOLDS.get('tolerancia_valor_total_abs', 0.50)
        if not all(c in df.columns for c in ['valor_total', 'valor_unitario', 'litros']):
            return res
        for _, row in df.iterrows():
            if not _ok(row, 'valor_total', 'valor_unitario', 'litros'):
                continue
            esperado = row['litros'] * row['valor_unitario']
            if esperado <= 0:
                continue
            diff = abs(row['valor_total'] - esperado)
            # Tolerância = máximo entre 2% do valor esperado e piso de R$0,50
            tol_efetiva = max(esperado * tol_pct, tol_abs)
            if diff > tol_efetiva:
                pct_diff = (diff / esperado) * 100
                res.append(_oc(
                    'R08',
                    'inconsistencia de valor total',
                    (f"Valor informado (R$ {row['valor_total']:.2f}) difere "
                     f"do esperado (R$ {esperado:.2f}) em R$ {diff:.2f} ({pct_diff:.1f}%)."),
                    'BAIXA', row,
                    f"R$ {row['valor_total']:.2f}",
                    f"R$ {esperado:.2f} ± {tol_efetiva:.2f} ({tol_pct*100:.0f}% ou R$ {tol_abs:.2f})",
                    (f"Qtde: {row['litros']:.2f} L | "
                     f"Vr. Unit.: R$ {row['valor_unitario']:.4f} | "
                     f"Esperado: R$ {esperado:.2f} | "
                     f"Informado: R$ {row['valor_total']:.2f} | "
                     f"Diferenca: R$ {diff:.2f} ({pct_diff:.1f}%) | "
                     f"Tolerancia: R$ {tol_efetiva:.2f}."),
                    "Verificar lancamento no sistema e confrontar com cupom fiscal.",
                ))
        return res

    # ------------------------------------------------------------------
    # R11 — Placa sem histórico suficiente para comparação estatística
    # ------------------------------------------------------------------
    def _r11_historico_insuficiente(self, df: pd.DataFrame) -> list:
        """Gera alerta BAIXA para placas cujo histórico foi enriquecido mas é insuficiente.
        Depende de 'contagem' (adicionada pelo AgentHistorico), portanto só ativa
        quando o recorte já passou pelo agent histórico — aqui é um aviso preventivo
        para placas que NÃO têm a coluna 'contagem' (sem histórico algum).
        """
        res = []
        min_hist = THRESHOLDS.get('minimo_historico_para_comparacao', 8)
        if 'placa' not in df.columns:
            return res
        # Se 'contagem' já existe (histórico processado), não duplicar aviso aqui
        if 'contagem' in df.columns:
            placas_sem_hist = (
                df[df['contagem'].isna() | (df['contagem'] < min_hist)]
                ['placa'].dropna().unique()
            )
        else:
            placas_sem_hist = df['placa'].dropna().unique()

        if len(placas_sem_hist) == 0:
            return res

        # Um alerta por placa, usando o primeiro registro
        emitidas = set()
        for _, row in df.iterrows():
            placa = row.get('placa', '')
            if placa not in placas_sem_hist or placa in emitidas:
                continue
            emitidas.add(placa)
            contagem = int(row.get('contagem', 0) or 0)
            contagem_total = int(row.get('contagem_total', 0) or 0)
            dias = THRESHOLDS.get('dias_historico_rolling', 90)

            # Distingue entre "sem registros" e "registros existem mas são inválidos"
            if contagem_total > 0 and contagem == 0:
                descricao = (
                    f"Placa {placa} possui {contagem_total} registro(s) nos ultimos {dias} dias, "
                    f"porem todos foram excluidos da comparacao estatistica por apresentarem "
                    f"consumo invalido (hodometro zerado ou leitura imposivel). "
                    f"Regras R09 e R10 nao foram aplicadas."
                )
                evidencia = f"{contagem_total} registros (0 validos para estatistica)"
            elif contagem_total > 0 and contagem < min_hist:
                descricao = (
                    f"Placa {placa} possui apenas {contagem} registro(s) valido(s) nos ultimos "
                    f"{dias} dias (minimo: {min_hist}). "
                    f"{contagem_total - contagem} registro(s) foram excluidos por consumo invalido. "
                    f"Regras R09 e R10 nao foram aplicadas."
                )
                evidencia = f"{contagem} validos de {contagem_total} registros"
            else:
                descricao = (
                    f"Placa {placa} possui apenas {contagem} registro(s) nos ultimos "
                    f"{dias} dias (minimo: {min_hist}). Regras R09 e R10 nao foram aplicadas."
                )
                evidencia = f"{contagem} registros historicos"

            res.append(_oc(
                'R11',
                'historico insuficiente para comparacao estatistica',
                descricao,
                'BAIXA', row,
                evidencia,
                f">= {min_hist} registros validos",
                (f"Placa: {placa} | "
                 f"Registros totais nos ultimos {dias} dias: {contagem_total} | "
                 f"Registros validos para estatistica: {contagem} | "
                 f"Minimo necessario: {min_hist}. "
                 f"Recomenda-se auditoria manual deste veiculo."),
                "Verificar consistencia do hodometro e solicitar documentacao de uso ao gestor responsavel.",
            ))
        return res
