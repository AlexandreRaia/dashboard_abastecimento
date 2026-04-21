import pandas as pd

from .config import GRAVIDADE_ORDEM


class AgentClassificacao:
    """
    Agente 5 — Classificacao e Priorizacao.
    Consolida múltiplas evidências por evento (placa + data_hora) e
    reclassifica a gravidade final com base em combinações.
    """

    # Conjuntos de códigos que elevam automaticamente para ALTA
    COMBOS_ALTA = [
        {'R03', 'R04'},  # rendimento critico + hodometro
        {'R04', 'R05'},  # hodometro + km incompativel
        {'R01', 'R03'},  # capacidade + rendimento critico
        {'R01', 'R05'},  # capacidade + km incompativel
    ]

    def processar(self, ocorrencias: list) -> list:
        if not ocorrencias:
            return []

        df = pd.DataFrame(ocorrencias)

        # Chave de agrupamento: mesmo abastecimento
        df['_chave'] = df['placa'].astype(str) + '|' + df['data_hora'].astype(str)

        resultado = []
        for chave, grupo in df.groupby('_chave', sort=False):
            codigos   = set(grupo['codigo_regra'].tolist())
            gravidades = grupo['gravidade_inicial'].tolist()

            # Gravidade base = maior gravidade inicial
            grav_max = max(gravidades, key=lambda g: GRAVIDADE_ORDEM.get(g, 0))

            # Dois alertas MEDIA no mesmo evento -> ALTA
            if gravidades.count('MEDIA') >= 2 and grav_max == 'MEDIA':
                grav_max = 'ALTA'

            # Combinações que forçam ALTA
            for combo in self.COMBOS_ALTA:
                if combo.issubset(codigos):
                    grav_max = 'ALTA'
                    break

            # Inconsistência de valor isolada permanece BAIXA
            if codigos == {'R08'}:
                grav_max = 'BAIXA'

            for _, row in grupo.iterrows():
                rec = row.to_dict()
                rec.pop('_chave', None)
                rec['gravidade_final']       = grav_max
                rec['qtd_evidencias_evento'] = len(grupo)
                rec['codigos_evento']        = ', '.join(sorted(codigos))
                resultado.append(rec)

        # Ordenar por gravidade final (ALTA primeiro) e depois por data
        return sorted(
            resultado,
            key=lambda x: (
                -GRAVIDADE_ORDEM.get(x.get('gravidade_final', 'BAIXA'), 0),
                str(x.get('data_hora', '')),
            ),
        )
