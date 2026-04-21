"""
Orquestrador principal — usa Agno para coordenar os agentes especialistas.

O pipeline é executado em sequência determinística:
  1. AgentIngestion      — padroniza a base
  2. AgentValidacao      — valida qualidade
  3. AgentRegras         — aplica regras objetivas
  4. AgentHistorico      — contexto histórico + R09/R10
  5. AgentClassificacao  — consolida gravidade final
  6. AgentRelatorio      — relatório técnico
  7. AgentNotificacao    — minutas administrativas

O Agno Agent fica disponível como interface de linguagem natural
sobre o pipeline (requer chave de API configurada no ambiente).
"""

import pandas as pd
from agno.agent import Agent
from agno.tools import tool

from .classification import AgentClassificacao
from .history import AgentHistorico
from .ingestion import AgentIngestion
from .notification import AgentNotificacao
from .report import AgentRelatorio
from .rules import AgentRegras
from .storage import AgentStorageSQLite
from .validation import AgentValidacao


# ---------------------------------------------------------------------------
# Instâncias dos agentes especialistas (stateless — reutilizáveis)
# ---------------------------------------------------------------------------
_ingestion      = AgentIngestion()
_validacao      = AgentValidacao()
_regras         = AgentRegras()
_historico      = AgentHistorico()
_classificacao  = AgentClassificacao()
_relatorio_agent = AgentRelatorio()
_notificacao    = AgentNotificacao()
_storage        = AgentStorageSQLite()


# ---------------------------------------------------------------------------
# Classe Orquestradora
# ---------------------------------------------------------------------------
class OrchestradorAuditoria:
    """
    Orquestrador principal do sistema multiagente de auditoria.
    Chame `run_pipeline(df_raw)` para executar a auditoria completa.
    """

    def __init__(self, metadata: dict = None):
        """
        metadata: informações do relatório (municipio, responsavel, secretaria).
        """
        self.metadata = metadata or {}

    # ------------------------------------------------------------------
    def run_pipeline(self, df_raw: pd.DataFrame, pre_validated: bool = False) -> dict:
        """
        Executa todos os agentes em sequência e retorna resultado consolidado.

        Retorna dict com chaves:
          log                 — lista de mensagens de progresso
          df_auditado         — DataFrame enriquecido (com historico)
          relatorio_qualidade — falhas de dados encontradas
          ocorrencias         — lista de dicts com todas as ocorrências classificadas
          relatorio           — dict estruturado do relatório técnico
          notificacoes        — lista de minutas administrativas
          relatorio_obj       — instância de AgentRelatorio (para gerar Excel)
        """
        log = []

        if pre_validated:
            log.append("▶ Fonte | Base historica validada (SQLite)")
            df_valido = df_raw.copy()
            rel_qualidade = []
            sqlite_info = {
                'status': 'NAO_APLICAVEL',
                'rows_written': 0,
                'rows_loaded': int(len(df_valido)),
                'db_path': self.metadata.get('db_path', ''),
                'table_name': self.metadata.get('db_table', ''),
            }
        else:
            # ── Agente 1: Ingestão ──────────────────────────────────────────
            log.append("▶ Agente 1 | Ingestao e Padronizacao")
            df_pad, rel_qualidade = _ingestion.processar(df_raw)
            linhas_apos_ingestao = len(df_pad)
            log.append(f"  ✔ {linhas_apos_ingestao} registros padronizados (entrada: {len(df_raw)}).")

            # ── Agente 2: Validação ─────────────────────────────────────────
            log.append("▶ Agente 2 | Validacao de Dados")
            df_valido, falhas = _validacao.processar(df_pad)
            linhas_apos_validacao = len(df_valido)
            descartados_validacao = linhas_apos_ingestao - linhas_apos_validacao
            rel_qualidade += falhas
            log.append(f"  ✔ {linhas_apos_validacao} registros aptos para auditoria (removidos: {descartados_validacao}).")

            # ── Persistência SQLite: base validada ─────────────────────────
            db_path = self.metadata.get('db_path', 'auditoria_frota.db')
            db_table = self.metadata.get('db_table', 'abastecimentos_validados')
            log.append("▶ Agente 2.5 | Persistencia SQLite")
            df_valido_db, sqlite_info = _storage.salvar_e_recarregar(
                df_valido,
                db_path=db_path,
                table_name=db_table,
            )
            df_valido = df_valido_db
            log.append(
                f"  ✔ SQLite OK | tabela={sqlite_info.get('table_name')} | "
                f"gravados={sqlite_info.get('rows_written', 0)} | "
                f"carregados={sqlite_info.get('rows_loaded', 0)}"
            )

        if df_valido.empty:
            log.append("  ✘ Nenhum registro valido. Pipeline encerrado.")
            return {
                'log':               log,
                'df_auditado':       df_valido,
                'relatorio_qualidade': rel_qualidade,
                'ocorrencias':       [],
                'relatorio':         {},
                'notificacoes':      [],
                'relatorio_obj':     _relatorio_agent,
                'sqlite':            sqlite_info,
            }

        # ── Agente 3: Regras operacionais ───────────────────────────────
        log.append("▶ Agente 3 | Motor de Regras Operacionais")
        regras_params = {
            'outlier_sigma_mult': self.metadata.get('outlier_sigma_mult', None),
        }
        ocs = _regras.processar(df_valido, params=regras_params)
        log.append(f"  ✔ {len(ocs)} ocorrencia(s) detectada(s) pelas regras basicas.")

        # ── Agente 4: Contexto histórico ────────────────────────────────
        log.append("▶ Agente 4 | Contexto Historico")
        df_enr, ocs = _historico.processar(df_valido, ocs)
        log.append(f"  ✔ {len(ocs)} ocorrencia(s) apos enriquecimento historico.")

        # ── Agente 5: Classificação ─────────────────────────────────────
        log.append("▶ Agente 5 | Classificacao e Priorizacao")
        ocs_class = _classificacao.processar(ocs)
        n_alta  = sum(1 for o in ocs_class if o.get('gravidade_final') == 'ALTA')
        n_media = sum(1 for o in ocs_class if o.get('gravidade_final') == 'MEDIA')
        n_baixa = len(ocs_class) - n_alta - n_media
        log.append(f"  ✔ ALTA: {n_alta} | MEDIA: {n_media} | BAIXA: {n_baixa}")

        # ── Agente 6: Relatório técnico ─────────────────────────────────
        log.append("▶ Agente 6 | Redator de Relatorios")
        relatorio = _relatorio_agent.processar(
            df_enr, ocs_class, rel_qualidade, self.metadata
        )
        log.append("  ✔ Relatorio tecnico gerado.")

        # ── Agente 7: Notificações ──────────────────────────────────────
        log.append("▶ Agente 7 | Comunicacao Administrativa")
        notifs = _notificacao.processar(ocs_class)
        log.append(f"  ✔ {len(notifs)} notificacao(es) gerada(s).")

        return {
            'log':               log,
            'df_auditado':       df_enr,
            'relatorio_qualidade': rel_qualidade,
            'ocorrencias':       ocs_class,
            'relatorio':         relatorio,
            'notificacoes':      notifs,
            'relatorio_obj':     _relatorio_agent,
            'sqlite':            sqlite_info,
        }

    def atualizar_base_diaria(self, df_raw: pd.DataFrame) -> dict:
        """
        Ingestao/validacao da planilha do dia e append deduplicado no SQLite historico.
        """
        log = []
        log.append("▶ Carga diaria | Ingestao e Padronizacao")
        df_pad, rel_qualidade = _ingestion.processar(df_raw)
        log.append(f"  ✔ {len(df_pad)} registro(s) padronizado(s).")

        log.append("▶ Carga diaria | Validacao")
        df_valido, falhas = _validacao.processar(df_pad)
        rel_qualidade += falhas
        log.append(f"  ✔ {len(df_valido)} registro(s) valido(s).")

        db_path = self.metadata.get('db_path', 'auditoria_frota.db')
        db_table_hist = self.metadata.get('db_table_historico', 'abastecimentos_historico')
        log.append("▶ Carga diaria | Persistencia historica (SQLite)")
        df_hist, sqlite_info = _storage.append_historico(
            df_valido,
            db_path=db_path,
            table_name=db_table_hist,
        )
        log.append(
            f"  ✔ Historico atualizado | inseridos={sqlite_info.get('rows_inserted', 0)} | "
            f"total={sqlite_info.get('rows_loaded', 0)}"
        )

        return {
            'log': log,
            'relatorio_qualidade': rel_qualidade,
            'sqlite': sqlite_info,
            'df_historico': df_hist,
        }

    def carregar_base_historica(self) -> tuple:
        """Carrega base historica validada a partir do SQLite."""
        db_path = self.metadata.get('db_path', 'auditoria_frota.db')
        db_table_hist = self.metadata.get('db_table_historico', 'abastecimentos_historico')
        return _storage.carregar_tabela(db_path=db_path, table_name=db_table_hist)

    def gerar_excel(self, resultado: dict) -> bytes:
        """Atalho para gerar o Excel a partir do resultado do pipeline."""
        return _relatorio_agent.gerar_excel(resultado['relatorio'])


# ---------------------------------------------------------------------------
# Ferramentas Agno (expostas como @tool para uso com LLM, opcional)
# ---------------------------------------------------------------------------

_ultimo_resultado: dict = {}


@tool
def executar_auditoria(caminho_excel: str) -> str:
    """
    Executa o pipeline completo de auditoria sobre um arquivo Excel.
    Retorna um resumo textual dos resultados.
    """
    global _ultimo_resultado
    try:
        df = pd.read_excel(caminho_excel)
    except Exception as e:
        return f"Erro ao ler arquivo: {e}"

    orq = OrchestradorAuditoria()
    _ultimo_resultado = orq.run_pipeline(df)

    r = _ultimo_resultado.get('relatorio', {})
    res = r.get('resumo_executivo', {})
    conclusao = r.get('conclusao', '')

    return (
        f"Auditoria concluida.\n"
        f"Registros analisados: {res.get('total_registros', 0)}\n"
        f"Placas: {res.get('total_placas', 0)} | Condutores: {res.get('total_condutores', 0)}\n"
        f"Ocorrencias ALTA: {res.get('ocorrencias_alta', 0)}\n"
        f"Ocorrencias MEDIA: {res.get('ocorrencias_media', 0)}\n"
        f"Ocorrencias BAIXA: {res.get('ocorrencias_baixa', 0)}\n\n"
        f"Conclusao: {conclusao}"
    )


@tool
def listar_ocorrencias_alta() -> str:
    """Lista as ocorrências de gravidade ALTA identificadas na última auditoria."""
    ocs = _ultimo_resultado.get('ocorrencias', [])
    altas = [o for o in ocs if o.get('gravidade_final') == 'ALTA']
    if not altas:
        return "Nenhuma ocorrencia ALTA identificada."
    linhas = []
    for o in altas[:20]:
        data = o.get('data_hora')
        data_str = data.strftime('%d/%m/%Y %H:%M') if hasattr(data, 'strftime') else str(data)
        linhas.append(
            f"[{o['codigo_regra']}] {o['placa']} | {o['condutor']} | {data_str} | "
            f"{o['tipo_ocorrencia']} | {o['descricao_tecnica']}"
        )
    return '\n'.join(linhas)


@tool
def gerar_notificacao_condutor(condutor: str) -> str:
    """Retorna a minuta de notificação para um condutor específico."""
    notifs = _ultimo_resultado.get('notificacoes', [])
    for n in notifs:
        if condutor.lower() in n.get('condutor', '').lower():
            return n['texto_notificacao']
    return f"Nenhuma notificacao encontrada para o condutor '{condutor}'."


# ---------------------------------------------------------------------------
# Agente Agno (interface de linguagem natural — requer modelo configurado)
# ---------------------------------------------------------------------------
def criar_agente_auditoria(model=None) -> Agent:
    """
    Cria e retorna um Agno Agent capaz de executar o pipeline via LLM.
    Passe um objeto de modelo Agno (ex.: OpenAIChat) como argumento.
    Se model=None, levanta ValueError orientando a configurar o modelo.
    """
    if model is None:
        raise ValueError(
            "Informe um modelo Agno. Exemplo:\n"
            "  from agno.models.openai import OpenAIChat\n"
            "  agente = criar_agente_auditoria(model=OpenAIChat(id='gpt-4o'))"
        )

    return Agent(
        model=model,
        tools=[executar_auditoria, listar_ocorrencias_alta, gerar_notificacao_condutor],
        instructions=(
            "Voce e um assistente tecnico especializado em auditoria de abastecimento de frota publica. "
            "Use as ferramentas disponíveis para executar auditorias, listar ocorrencias e gerar notificacoes. "
            "Seja tecnico, objetivo e imparcial. Nunca afirme fraude como fato consumado; "
            "use termos como indicio, inconsistencia ou desvio operacional."
        ),
        markdown=True,
    )
