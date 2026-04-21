import pandas as pd

from .config import TANK_CAPACITY, THRESHOLDS


# ---------------------------------------------------------------------------
# Helper: monta um dicionário de ocorrência padronizado
# ---------------------------------------------------------------------------
def _oc(codigo, tipo, descricao, gravidade, row,
        valor_observado, valor_referencia, evidencia, recomendacao):
    data = row.get('data_hora')
    litros = row.get('litros')
    media_modelo = row.get('media_consumo_modelo')
    km_esperado = (litros * media_modelo) if pd.notna(litros) and pd.notna(media_modelo) else pd.NA
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
        'media':           media_modelo if pd.notna(media_modelo) else pd.NA,
        'min':             row.get('km_minimo', pd.NA),
        'max':             row.get('km_maximo', pd.NA),
        'desvio':          row.get('desvio_consumo_modelo', pd.NA),
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
        ocs += self._r01_capacidade_tanque(df)
        ocs += self._r02_consumo_fora_faixa(df)
        ocs += self._r03_consumo_critico(df, sigma_override=params.get('outlier_sigma_mult'))
        ocs += self._r04_hodometro(df)
        ocs += self._r05_km_incompativel(df)
        ocs += self._r06_abastecimentos_proximos(df)
        ocs += self._r07_preco_acima_contratado(df)
        ocs += self._r08_valor_inconsistente(df)
        # R09 e R10 são gerados pelo AgentHistorico com dados de contexto
        return ocs

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
    def _r03_consumo_critico(self, df: pd.DataFrame, sigma_override=None) -> list:
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

        # Estatisticas por modelo para detectar outliers negativos de consumo.
        if 'modelo_norm' in df.columns:
            stats = (
                df.groupby('modelo_norm', dropna=False)['consumo']
                .agg(['mean', 'std', 'count'])
                .reset_index()
                .rename(columns={
                    'mean': 'media_consumo_modelo',
                    'std': 'desvio_consumo_modelo',
                    'count': 'qtd_modelo',
                })
            )
            df_ref = df.merge(stats, on='modelo_norm', how='left')
        else:
            df_ref = df.copy()
            df_ref['media_consumo_modelo'] = pd.NA
            df_ref['desvio_consumo_modelo'] = pd.NA
            df_ref['qtd_modelo'] = 0

        for _, row in df_ref.iterrows():
            if not _ok(row, 'consumo'):
                continue

            c = row['consumo']

            qtd_modelo = row.get('qtd_modelo', 0)
            media_m = row.get('media_consumo_modelo')
            desvio_m = row.get('desvio_consumo_modelo')

            usa_outlier = (
                pd.notna(media_m)
                and pd.notna(desvio_m)
                and desvio_m > 0
                and qtd_modelo >= min_amostra
            )

            if usa_outlier:
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
    # ------------------------------------------------------------------
    def _r08_valor_inconsistente(self, df: pd.DataFrame) -> list:
        res = []
        tol = THRESHOLDS['tolerancia_valor_total']
        if not all(c in df.columns for c in ['valor_total', 'valor_unitario', 'litros']):
            return res
        for _, row in df.iterrows():
            if not _ok(row, 'valor_total', 'valor_unitario', 'litros'):
                continue
            esperado = row['litros'] * row['valor_unitario']
            diff = abs(row['valor_total'] - esperado)
            if diff > tol:
                res.append(_oc(
                    'R08',
                    'inconsistencia de valor total',
                    (f"Valor informado (R$ {row['valor_total']:.2f}) difere "
                     f"do esperado (R$ {esperado:.2f}) em R$ {diff:.2f}."),
                    'BAIXA', row,
                    f"R$ {row['valor_total']:.2f}",
                    f"R$ {esperado:.2f} (Qtde x Vr. Unit.)",
                    (f"Qtde: {row['litros']:.2f} L | "
                     f"Vr. Unit.: R$ {row['valor_unitario']:.4f} | "
                     f"Esperado: R$ {esperado:.2f} | "
                     f"Informado: R$ {row['valor_total']:.2f} | "
                     f"Diferenca: R$ {diff:.2f}."),
                    "Verificar lancamento no sistema e confrontar com cupom fiscal.",
                ))
        return res
