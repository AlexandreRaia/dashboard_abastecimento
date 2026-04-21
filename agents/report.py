from datetime import datetime
from io import BytesIO

import pandas as pd

from .config import GRAVIDADE_ORDEM


class AgentRelatorio:
    """
    Agente 6 — Redator de Relatórios.
    Gera o relatório técnico formal inspirado no Modelo_relatorio institucional.
    Produz dicionário estruturado e exporta para Excel.
    """

    def processar(
        self,
        df: pd.DataFrame,
        ocorrencias: list,
        falhas_qualidade: list,
        metadata: dict = None,
    ) -> dict:
        meta  = metadata or {}
        now   = datetime.now()
        df_oc = pd.DataFrame(ocorrencias) if ocorrencias else pd.DataFrame()

        altas  = df_oc[df_oc['gravidade_final'] == 'ALTA']  if not df_oc.empty else pd.DataFrame()
        medias = df_oc[df_oc['gravidade_final'] == 'MEDIA'] if not df_oc.empty else pd.DataFrame()

        resumo = self._resumo(df, df_oc, meta)

        relatorio = {
            'cabecalho': {
                'titulo':       'RELATORIO TECNICO DE ANALISE DE ABASTECIMENTO DE FROTA',
                'data_geracao': now.strftime('%d/%m/%Y %H:%M'),
                'municipio':    meta.get('municipio', ''),
                'responsavel':  meta.get('responsavel', 'Departamento de Transportes'),
                'secretaria':   meta.get('secretaria', ''),
            },
            'resumo_executivo':       resumo,
            'qualidade_dados':        [f for f in falhas_qualidade
                                       if f.get('gravidade') not in ('INFORMATIVO',)],
            'ocorrencias_por_tipo':   self._por_tipo(df_oc),
            'ocorrencias_por_gravidade': self._por_gravidade(df_oc),
            'ranking_placa':          self._ranking(df_oc, 'placa'),
            'ranking_condutor':       self._ranking(df_oc, 'condutor'),
            'ranking_estabelecimento': self._ranking(df_oc, 'estabelecimento'),
            'ocorrencias_alta':       altas.to_dict('records')  if not altas.empty  else [],
            'ocorrencias_media':      medias.to_dict('records') if not medias.empty else [],
            'todas_ocorrencias':      df_oc.to_dict('records')  if not df_oc.empty  else [],
            'conclusao':              self._conclusao(df_oc, resumo),
        }
        return relatorio

    # ------------------------------------------------------------------
    def _resumo(self, df: pd.DataFrame, df_oc: pd.DataFrame, meta: dict) -> dict:
        periodo_ini = str(df['data_hora'].min()) if 'data_hora' in df.columns else ''
        periodo_fim = str(df['data_hora'].max()) if 'data_hora' in df.columns else ''
        return {
            'total_registros':       len(df),
            'total_placas':          df['placa'].nunique()    if 'placa'    in df.columns else 0,
            'total_condutores':      df['condutor'].nunique() if 'condutor' in df.columns else 0,
            'total_litros':          float(df['litros'].sum()) if 'litros'  in df.columns else 0,
            'total_valor':           float(df['valor_total'].sum()) if 'valor_total' in df.columns else 0,
            'periodo_inicio':        periodo_ini,
            'periodo_fim':           periodo_fim,
            'total_ocorrencias':     len(df_oc),
            'ocorrencias_alta':      int((df_oc['gravidade_final'] == 'ALTA').sum())  if not df_oc.empty else 0,
            'ocorrencias_media':     int((df_oc['gravidade_final'] == 'MEDIA').sum()) if not df_oc.empty else 0,
            'ocorrencias_baixa':     int((df_oc['gravidade_final'] == 'BAIXA').sum()) if not df_oc.empty else 0,
        }

    def _por_tipo(self, df_oc: pd.DataFrame) -> dict:
        if df_oc.empty or 'tipo_ocorrencia' not in df_oc.columns:
            return {}
        return df_oc.groupby('tipo_ocorrencia').size().sort_values(ascending=False).to_dict()

    def _por_gravidade(self, df_oc: pd.DataFrame) -> dict:
        if df_oc.empty or 'gravidade_final' not in df_oc.columns:
            return {}
        return df_oc.groupby('gravidade_final').size().to_dict()

    def _ranking(self, df_oc: pd.DataFrame, campo: str) -> list:
        if df_oc.empty or campo not in df_oc.columns or 'gravidade_final' not in df_oc.columns:
            return []
        rank = (
            df_oc.groupby(campo)
            .agg(
                total_ocorrencias=(campo, 'count'),
                altas=('gravidade_final', lambda x: (x == 'ALTA').sum()),
                medias=('gravidade_final', lambda x: (x == 'MEDIA').sum()),
            )
            .sort_values(['altas', 'total_ocorrencias'], ascending=False)
            .reset_index()
        )
        return rank.head(10).to_dict('records')

    def _conclusao(self, df_oc: pd.DataFrame, resumo: dict) -> str:
        if df_oc.empty:
            return "Nenhuma ocorrencia identificada no recorte analisado."
        n_alta  = resumo['ocorrencias_alta']
        n_media = resumo['ocorrencias_media']
        placas_alta = (
            df_oc[df_oc['gravidade_final'] == 'ALTA']['placa'].unique().tolist()
            if n_alta > 0 else []
        )
        txt = (
            f"A analise identificou {len(df_oc)} ocorrencia(s), sendo "
            f"{n_alta} de gravidade ALTA e {n_media} de gravidade MEDIA. "
        )
        if placas_alta:
            txt += f"Veiculos com ocorrencias ALTA: {', '.join(placas_alta)}. "
        txt += (
            "Recomenda-se notificacao formal dos condutores envolvidos e "
            "apuracao detalhada dos casos de gravidade ALTA, "
            "assegurados o contraditorio e a ampla defesa."
        )
        return txt

    # ------------------------------------------------------------------
    # Exportação Excel
    # ------------------------------------------------------------------
    def gerar_excel(self, relatorio: dict) -> bytes:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:

            # Resumo executivo
            resumo_rows = [{'Indicador': k, 'Valor': v}
                           for k, v in relatorio['resumo_executivo'].items()]
            pd.DataFrame(resumo_rows).to_excel(writer, sheet_name='Resumo', index=False)

            # Ocorrências ALTA
            if relatorio['ocorrencias_alta']:
                pd.DataFrame(relatorio['ocorrencias_alta']).to_excel(
                    writer, sheet_name='Ocorrencias ALTA', index=False)

            # Ocorrências MEDIA
            if relatorio['ocorrencias_media']:
                pd.DataFrame(relatorio['ocorrencias_media']).to_excel(
                    writer, sheet_name='Ocorrencias MEDIA', index=False)

            # Todas as ocorrências
            if relatorio['todas_ocorrencias']:
                pd.DataFrame(relatorio['todas_ocorrencias']).to_excel(
                    writer, sheet_name='Todas Ocorrencias', index=False)

            # Rankings
            for campo, label in [
                ('placa',          'Ranking Placas'),
                ('condutor',       'Ranking Condutores'),
                ('estabelecimento', 'Ranking Estabelecimentos'),
            ]:
                dados = relatorio.get(f'ranking_{campo}', [])
                if dados:
                    pd.DataFrame(dados).to_excel(writer, sheet_name=label, index=False)

            # Qualidade de dados
            if relatorio.get('qualidade_dados'):
                pd.DataFrame(relatorio['qualidade_dados']).to_excel(
                    writer, sheet_name='Qualidade Dados', index=False)

        output.seek(0)
        return output.getvalue()
