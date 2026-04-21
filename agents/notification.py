from datetime import datetime


class AgentNotificacao:
    """
    Agente 7 — Comunicacao Administrativa.
    Gera minutas de notificação formal para ocorrências MEDIA ou ALTA.
    Agrupa por condutor + placa + data para evitar duplicidade.
    """

    THRESHOLD_GRAVIDADE = {'MEDIA', 'ALTA'}

    def processar(self, ocorrencias: list) -> list:
        """
        Retorna lista de dicionários com texto de notificação pronto.
        """
        grupos: dict = {}

        for oc in ocorrencias:
            grav = oc.get('gravidade_final', oc.get('gravidade_inicial', 'BAIXA'))
            if grav not in self.THRESHOLD_GRAVIDADE:
                continue

            condutor  = oc.get('condutor', 'Nao identificado')
            placa     = oc.get('placa', '?')
            unidade   = (oc.get('unidade') or '').strip()
            data_hora = oc.get('data_hora')
            data_str  = (
                data_hora.strftime('%d/%m/%Y %H:%M')
                if hasattr(data_hora, 'strftime')
                else str(data_hora)
            )

            # Inclui unidade na chave para preservar o vinculo do veiculo no agrupamento.
            chave = f"{condutor}|{placa}|{unidade}|{data_str}"
            if chave not in grupos:
                grupos[chave] = {
                    'condutor':      condutor,
                    'placa':         placa,
                    'modelo':        oc.get('modelo', ''),
                    'unidade':       unidade,
                    'estabelecimento': oc.get('estabelecimento', ''),
                    'data_str':      data_str,
                    'ocorrencias':   [],
                    'gravidade_max': grav,
                }
            grupos[chave]['ocorrencias'].append(oc)
            if grav == 'ALTA':
                grupos[chave]['gravidade_max'] = 'ALTA'

        notificacoes = []
        for grupo in grupos.values():
            linhas_oc = '\n'.join(
                f"  [{oc.get('codigo_regra','?')}] "
                f"{oc.get('tipo_ocorrencia','').title()}: "
                f"{oc.get('descricao_tecnica','')}"
                for oc in grupo['ocorrencias']
            )

            unidade_txt = f" ({grupo['unidade']})" if grupo['unidade'] else ''
            modelo_txt  = f" ({grupo['modelo']})"  if grupo['modelo']  else ''

            texto = (
                f"NOTIFICACAO ADMINISTRATIVA — CONTROLE DE FROTA\n"
                f"Data de emissao: {datetime.now().strftime('%d/%m/%Y')}\n"
                f"{'=' * 60}\n\n"
                f"Prezado(a) {grupo['condutor']}{unidade_txt},\n\n"
                f"Foram identificadas inconsistencias tecnicas nos registros de "
                f"abastecimento vinculados ao veiculo {grupo['placa']}{modelo_txt}, "
                f"realizado em {grupo['data_str']} "
                f"no estabelecimento {grupo['estabelecimento'] or 'nao informado'}.\n\n"
                f"OCORRENCIAS IDENTIFICADAS:\n"
                f"{linhas_oc}\n\n"
                f"Diante do exposto, solicitamos manifestacao formal com apresentacao "
                f"de justificativas e documentos comprobatorios no prazo de "
                f"48 (quarenta e oito) horas a contar do recebimento desta notificacao.\n\n"
                f"Ressaltamos que a presente analise possui carater tecnico e preliminar, "
                f"sendo assegurados ao notificado os principios do contraditorio e da "
                f"ampla defesa, nos termos da legislacao vigente.\n\n"
                f"Atenciosamente,\n"
                f"Fiscalizacao de Frota\n"
                f"Departamento de Transportes\n"
            )

            notificacoes.append({
                'condutor':           grupo['condutor'],
                'unidade':            grupo['unidade'],
                'placa':              grupo['placa'],
                'modelo':             grupo['modelo'],
                'data_abastecimento': grupo['data_str'],
                'estabelecimento':    grupo['estabelecimento'],
                'gravidade_max':      grupo['gravidade_max'],
                'qtd_ocorrencias':    len(grupo['ocorrencias']),
                'texto_notificacao':  texto,
                'ocorrencias':        grupo['ocorrencias'],
            })

        # ALTA primeiro
        return sorted(notificacoes, key=lambda n: n['gravidade_max'] == 'ALTA', reverse=True)
