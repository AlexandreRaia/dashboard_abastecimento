import pandas as pd

from .config import THRESHOLDS


class AgentValidacao:
    """
    Agente 2 — Validacao de Dados.
    Verifica qualidade antes da auditoria e separa o que é problema de dados
    do que é ocorrencia auditavel.
    """

    COLUNAS_OBRIGATORIAS = ['data_hora', 'placa', 'condutor', 'litros', 'km_rodado', 'modelo']

    def processar(self, df: pd.DataFrame) -> tuple:
        """
        Retorna (df_auditavel, lista_falhas).
        df_auditavel contém apenas registros com dados mínimos válidos.
        """
        falhas = []
        mask_valido = pd.Series(True, index=df.index)

        # Colunas obrigatórias ausentes
        ausentes = [c for c in self.COLUNAS_OBRIGATORIAS if c not in df.columns]
        if ausentes:
            falhas.append({
                'tipo': 'COLUNAS_CRITICAS_AUSENTES',
                'detalhe': f"Colunas criticas ausentes no DataFrame: {', '.join(ausentes)}",
                'quantidade': 0,
                'gravidade': 'CRITICA',
            })

        # Nulos em campos críticos
        for col in [c for c in self.COLUNAS_OBRIGATORIAS if c in df.columns]:
            nulos = int(df[col].isna().sum())
            if nulos:
                mask_valido &= df[col].notna()
                falhas.append({
                    'tipo': 'NULOS_CRITICOS',
                    'detalhe': f"Campo '{col}': {nulos} registro(s) nulo(s) removidos da base auditavel.",
                    'quantidade': nulos,
                    'gravidade': 'MEDIA',
                })

        # Litros inválidos — remove da base (sem dado de abastecimento, registro inútil)
        if 'litros' in df.columns:
            inv = df['litros'] <= 0
            if inv.any():
                qtd = int(inv.sum())
                mask_valido &= ~inv
                falhas.append({
                    'tipo': 'LITROS_INVALIDOS',
                    'detalhe': f"{qtd} registro(s) com Qtde (L) <= 0 removidos.",
                    'quantidade': qtd,
                    'gravidade': 'MEDIA',
                })

        # ── Erros de quilometragem: ficam na base, ganham flag _erro_km ──────
        # A flag impede que esses registros apareçam em gráficos e estatísticas
        # de consumo, mas o registro é preservado para o relatório de qualidade.
        df = df.copy()
        if '_erro_km' not in df.columns:
            df['_erro_km'] = False

        # Km rodado negativo
        if 'km_rodado' in df.columns:
            inv = df['km_rodado'] < 0
            if inv.any():
                qtd = int(inv.sum())
                df.loc[inv, '_erro_km'] = True
                df.loc[inv, 'km_rodado'] = 0  # zera para não propagar cálculo
                falhas.append({
                    'tipo': 'KM_RODADO_NEGATIVO',
                    'detalhe': (
                        f"{qtd} registro(s) com Km Rodado negativo. "
                        f"Provável erro de digitação — mantidos no relatório, excluídos dos gráficos."
                    ),
                    'quantidade': qtd,
                    'gravidade': 'MEDIA',
                })

        # Km rodado absurdamente alto (provável erro de digitação)
        if 'km_rodado' in df.columns:
            _km_max = THRESHOLDS.get('km_rodado_max_valido', 2000)
            inv = df['km_rodado'].notna() & (df['km_rodado'] > _km_max)
            if inv.any():
                qtd = int(inv.sum())
                df.loc[inv, '_erro_km'] = True
                df.loc[inv, 'km_rodado'] = 0
                falhas.append({
                    'tipo': 'KM_RODADO_IMPROVAVEL',
                    'detalhe': (
                        f"{qtd} registro(s) com Km Rodado > {_km_max:,.0f} km. "
                        f"Provável erro de digitação — mantidos no relatório, excluídos dos gráficos."
                    ),
                    'quantidade': qtd,
                    'gravidade': 'MEDIA',
                })

        # Km atual (odômetro) com valor impossível
        if 'km_atual' in df.columns:
            _km_odo_max = THRESHOLDS.get('km_atual_max_valido', 999_999)
            inv = df['km_atual'].notna() & (df['km_atual'] > _km_odo_max)
            if inv.any():
                qtd = int(inv.sum())
                df.loc[inv, '_erro_km'] = True
                falhas.append({
                    'tipo': 'KM_ATUAL_IMPOSSIVEL',
                    'detalhe': (
                        f"{qtd} registro(s) com odômetro > {_km_odo_max:,} km. "
                        f"Leitura fisicamente impossível — mantidos no relatório, excluídos dos gráficos."
                    ),
                    'quantidade': qtd,
                    'gravidade': 'MEDIA',
                })

        # Retrocesso de hodômetro (km_atual < ult_km)
        if 'km_atual' in df.columns and 'ult_km' in df.columns:
            retrocesso = (
                df['km_atual'].notna()
                & df['ult_km'].notna()
                & (df['km_atual'] > 0)
                & (df['ult_km'] > 0)
                & (df['km_atual'] < df['ult_km'])
            )
            if retrocesso.any():
                qtd = int(retrocesso.sum())
                df.loc[retrocesso, '_erro_km'] = True
                df.loc[retrocesso, 'km_rodado'] = 0
                falhas.append({
                    'tipo': 'HODOMETRO_RETROCEDIDO',
                    'detalhe': (
                        f"{qtd} registro(s) com km Atual < km Anterior (hodômetro retrocedido). "
                        f"Mantidos no relatório, excluídos dos gráficos de consumo."
                    ),
                    'quantidade': qtd,
                    'gravidade': 'MEDIA',
                })

        # Valor negativo — remove da base
        if 'valor_total' in df.columns:
            inv = df['valor_total'].notna() & (df['valor_total'] < 0)
            if inv.any():
                qtd = int(inv.sum())
                mask_valido &= ~inv
                falhas.append({
                    'tipo': 'VALOR_NEGATIVO',
                    'detalhe': f"{qtd} registro(s) com Valor negativo removidos.",
                    'quantidade': qtd,
                    'gravidade': 'MEDIA',
                })

        # Duplicatas
        chave_dup = [c for c in ['placa', 'data_hora', 'litros'] if c in df.columns]
        if len(chave_dup) == 3:
            dup = df[mask_valido].duplicated(subset=chave_dup, keep=False)
            if dup.any():
                falhas.append({
                    'tipo': 'DUPLICATAS',
                    'detalhe': f"{int(dup.sum())} registro(s) duplicados (mesma placa + data + litros).",
                    'quantidade': int(dup.sum()),
                    'gravidade': 'BAIXA',
                })

        df_valido = df[mask_valido].copy().reset_index(drop=True)

        # ── Consumo calculado fora da faixa fisiológica ──────────────────────
        # O consumo é calculado no AgentIngestion (km_rodado / litros), portanto
        # só pode ser validado aqui, depois da ingestão.
        # Consumo > 100 km/L: hodômetro provavelmente zerado/errado — marca _erro_km.
        # Consumo < 0.5 km/L: litros absurdamente altos ou km quase zero — marca _erro_km.
        # Em ambos os casos o registro é mantido para o relatório de qualidade.
        _consumo_max = float(THRESHOLDS.get('consumo_max_valido', 100.0))
        _consumo_min = float(THRESHOLDS.get('consumo_min_valido', 0.5))
        if 'consumo' in df_valido.columns:
            _consumo_num = pd.to_numeric(df_valido['consumo'], errors='coerce')
            _consumo_alto = _consumo_num.notna() & (_consumo_num > _consumo_max)
            _consumo_baixo = _consumo_num.notna() & (_consumo_num > 0) & (_consumo_num < _consumo_min)
            if '_erro_km' not in df_valido.columns:
                df_valido['_erro_km'] = False
            # Garante dtype bool antes de qualquer atribuição (coluna pode vir como string do SQLite)
            df_valido['_erro_km'] = df_valido['_erro_km'].map(
                lambda v: v if isinstance(v, bool) else str(v).strip().lower() == 'true'
            ).astype(bool)
            if _consumo_alto.any():
                qtd = int(_consumo_alto.sum())
                df_valido.loc[_consumo_alto, '_erro_km'] = True
                falhas.append({
                    'tipo': 'CONSUMO_IMPOSSIVEL_ALTO',
                    'detalhe': (
                        f"{qtd} registro(s) com consumo > {_consumo_max:.0f} km/L "
                        f"(provável erro de hodômetro) — mantidos no relatório, excluídos dos gráficos."
                    ),
                    'quantidade': qtd,
                    'gravidade': 'MEDIA',
                })
            if _consumo_baixo.any():
                qtd = int(_consumo_baixo.sum())
                # Não marca _erro_km — consumo baixo pode ser legítimo (baixa eficiência,
                # trajeto curto, veículo pesado). Mantém na análise dos agentes e no gráfico.
                falhas.append({
                    'tipo': 'CONSUMO_BAIXO_OBSERVADO',
                    'detalhe': (
                        f"{qtd} registro(s) com consumo entre 0 e {_consumo_min:.1f} km/L "
                        f"(possível km não atualizado ou veículo com baixa eficiência) — mantidos na análise."
                    ),
                    'quantidade': qtd,
                    'gravidade': 'BAIXA',
                })

        # Garantir que _erro_km seja sempre bool (pode chegar como string via SQLite)
        if '_erro_km' in df_valido.columns:
            df_valido['_erro_km'] = df_valido['_erro_km'].astype(bool)
        n_erro_km = int(df_valido['_erro_km'].sum()) if '_erro_km' in df_valido.columns else 0
        falhas.append({
            'tipo': 'VALIDACAO_CONCLUIDA',
            'detalhe': (
                f"Registros na base auditável: {len(df_valido)} de {len(df)} "
                f"({n_erro_km} marcados como erro de digitação de km — excluídos apenas dos gráficos)."
            ),
            'quantidade': len(df_valido),
            'gravidade': 'INFORMATIVO',
        })

        return df_valido, falhas
