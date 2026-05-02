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
            # Mesmo com base pré-validada, inspeciona consumo calculado para
            # detectar km/L impossíveis (> consumo_max_valido) e gerar ocorrências.
            log.append("▶ Agente 2 | Validacao de Consumo (base pre-validada)")
            _, falhas_consumo = _validacao.processar(df_valido)
            rel_qualidade += [f for f in falhas_consumo if f.get('tipo', '').startswith('CONSUMO')]
            if rel_qualidade:
                log.append(f"  ⚠ {len(rel_qualidade)} falha(s) de consumo detectada(s).")
            # Propagar _erro_km da validação de consumo para df_valido
            import pandas as _pd
            _consumo_max = float(__import__('agents.config', fromlist=['THRESHOLDS']).THRESHOLDS.get('consumo_max_valido', 30.0))
            if 'consumo' in df_valido.columns:
                _consumo_num = _pd.to_numeric(df_valido['consumo'], errors='coerce')
                _mask_alto = _consumo_num.notna() & (_consumo_num > _consumo_max)
                if '_erro_km' not in df_valido.columns:
                    df_valido['_erro_km'] = False
                # Converter para bool antes da atribuição (coluna pode vir como string do SQLite)
                df_valido['_erro_km'] = df_valido['_erro_km'].map(
                    lambda v: v if isinstance(v, bool) else str(v).strip().lower() == 'true'
                ).astype(bool)
                df_valido.loc[_mask_alto, '_erro_km'] = True
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
            }

        # ── Agente 3: Regras operacionais ───────────────────────────────
        log.append("▶ Agente 3 | Motor de Regras Operacionais")
        # Preparar histórico de 90 dias para o R03 calcular estatísticas por modelo
        _df_hist_completo = self.metadata.get('df_historico_completo')
        _df_hist_modelo = None
        if _df_hist_completo is not None and not _df_hist_completo.empty:
            from .config import THRESHOLDS
            import pandas as _pd
            # df_historico_completo vem do repositório (nomes já normalizados:
            # km_rodado, litros, modelo, data_hora, placa) — sem rename necessário.
            _df_h = _df_hist_completo.copy()
            if 'data_hora' in _df_h.columns:
                _df_h['data_hora'] = _pd.to_datetime(_df_h['data_hora'], errors='coerce')
                _data_max = _df_h['data_hora'].max()
                if _pd.notna(_data_max):
                    _df_h = _df_h[_df_h['data_hora'] >= _data_max - _pd.Timedelta(days=int(THRESHOLDS.get('dias_historico_rolling', 90)))]
            # Calcular consumo se não existir
            if 'consumo' not in _df_h.columns and 'km_rodado' in _df_h.columns and 'litros' in _df_h.columns:
                _df_h['consumo'] = _pd.to_numeric(_df_h['km_rodado'], errors='coerce') / _pd.to_numeric(_df_h['litros'], errors='coerce').replace(0, float('nan'))
            if 'modelo_norm' not in _df_h.columns and 'modelo' in _df_h.columns:
                from .config import normalizar_texto
                _df_h['modelo_norm'] = _df_h['modelo'].apply(normalizar_texto)
            _df_hist_modelo = _df_h
        regras_params = {
            'outlier_sigma_mult':    self.metadata.get('outlier_sigma_mult', None),
            'df_historico_modelo':   _df_hist_modelo,
        }
        ocs = _regras.processar(df_valido, params=regras_params)
        log.append(f"  ✔ {len(ocs)} ocorrencia(s) detectada(s) pelas regras basicas.")

        # ── Agente 4: Contexto histórico ────────────────────────────────
        log.append("▶ Agente 4 | Contexto Historico")
        df_historico_completo = self.metadata.get('df_historico_completo')
        
        # df_historico_completo vem do repositório com colunas já normalizadas
        # (placa, litros, km_rodado, data_hora, consumo…) — AgentHistorico usa
        # esses mesmos nomes internamente, sem necessidade de rename.
        df_enr, ocs = _historico.processar(df_valido, ocs, df_historico=df_historico_completo)
        # R11 executado aqui — df_enr já tem coluna 'contagem' populada pelo AgentHistorico
        ocs += _regras._r11_historico_insuficiente(df_enr)
        log.append(f"  ✔ {len(ocs)} ocorrencia(s) apos enriquecimento historico (incl. R11).")

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

        # Economia estimada: soma do valor_total dos abastecimentos com ao menos uma ocorrência ALTA.
        # Usa (placa, Timestamp normalizado) para evitar mismatch de representação de string.
        _ts_norm = lambda v: pd.Timestamp(v).floor('min') if pd.notna(v) else pd.NaT  # noqa: E731
        _altas_keys = {
            (str(o.get('placa', '')), _ts_norm(o.get('data_hora')))
            for o in ocs_class
            if o.get('gravidade_final') == 'ALTA' and pd.notna(o.get('data_hora'))
        }
        economia_estimada = 0.0
        if _altas_keys and 'valor_total' in df_enr.columns:
            _ts_col = pd.to_datetime(df_enr['data_hora'], errors='coerce').dt.floor('min')
            _mask_alta = [
                (str(pl), ts) in _altas_keys
                for pl, ts in zip(df_enr['placa'].fillna('').astype(str), _ts_col)
            ]
            economia_estimada = float(
                pd.to_numeric(df_enr.loc[_mask_alta, 'valor_total'], errors='coerce')
                .fillna(0.0).sum()
            )

        return {
            'log':                log,
            'df_auditado':        df_enr,
            'relatorio_qualidade': rel_qualidade,
            'ocorrencias':        ocs_class,
            'relatorio':          relatorio,
            'notificacoes':       notifs,
            'relatorio_obj':      _relatorio_agent,
            'economia_estimada':  economia_estimada,
        }

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
