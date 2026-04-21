import pandas as pd


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

        # Litros inválidos
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

        # Km rodado negativo
        if 'km_rodado' in df.columns:
            inv = df['km_rodado'] < 0
            if inv.any():
                qtd = int(inv.sum())
                mask_valido &= ~inv
                falhas.append({
                    'tipo': 'KM_RODADO_NEGATIVO',
                    'detalhe': f"{qtd} registro(s) com Km Rodado negativo removidos.",
                    'quantidade': qtd,
                    'gravidade': 'MEDIA',
                })

        # Valor negativo
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
        falhas.append({
            'tipo': 'VALIDACAO_CONCLUIDA',
            'detalhe': (
                f"Registros aptos para auditoria: {len(df_valido)} de {len(df)}. "
                f"Descartados: {len(df) - len(df_valido)}."
            ),
            'quantidade': len(df_valido),
            'gravidade': 'INFORMATIVO',
        })

        return df_valido, falhas
