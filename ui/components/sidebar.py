"""
Sidebar do dashboard de abastecimento.

Responsável por:
1. Exibir a identidade da aplicação (brand)
2. Upload de arquivo Excel / atualização via API Sisatec
3. Filtros de período, secretaria e combustível
4. Painel de alertas de limite
5. Botão para abrir o editor de parâmetros financeiros
"""
from __future__ import annotations

import io
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from config import settings, MONTHS
from core.utils.formatters import currency
from infrastructure.api.sisatec_client import SisatecClient
from infrastructure.repositories.abastecimento_repo import import_excel, load_abastecimentos


# ---------------------------------------------------------------------------
# Painel de alertas
# ---------------------------------------------------------------------------

def _render_alerts(alerts: dict) -> None:
    """
    Renderiza os cards de alerta (vermelho/amarelo/verde) na sidebar.

    Args:
        alerts: Dicionário retornado por build_alerts().
    """
    vermelhos = alerts.get("vermelhos", [])
    amarelos  = alerts.get("amarelos",  [])
    verdes    = alerts.get("verdes",    [])

    def _detalhe(items: list, limit: int = 3) -> str:
        if not items:
            return ""
        top = items[:limit]
        resto = len(items) - limit
        names = ", ".join(str(s) for s in top)
        return names + (f" +{resto} mais" if resto > 0 else "")

    html = f"""
    <div class="alert-card alert-red">
        <div class="alert-icon">🔴</div>
        <div class="alert-body">
            <div class="alert-count-red">{len(vermelhos)}</div>
            <div class="alert-label">Secretarias acima do limite</div>
            <div class="alert-detail">{_detalhe(vermelhos)}</div>
        </div>
    </div>
    <div class="alert-card alert-yellow">
        <div class="alert-icon">🟡</div>
        <div class="alert-body">
            <div class="alert-count-yellow">{len(amarelos)}</div>
            <div class="alert-label">Secretarias em atenção (>80%)</div>
            <div class="alert-detail">{_detalhe(amarelos)}</div>
        </div>
    </div>
    <div class="alert-card alert-green">
        <div class="alert-icon">🟢</div>
        <div class="alert-body">
            <div class="alert-count-green">{len(verdes)}</div>
            <div class="alert-label">Secretarias dentro do limite</div>
            <div class="alert-detail">{_detalhe(verdes)}</div>
        </div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Componente principal da sidebar
# ---------------------------------------------------------------------------

def render_sidebar(df_full: pd.DataFrame) -> tuple[
    pd.DataFrame,         # df filtrado
    date | None,          # data_inicio
    date | None,          # data_fim
    list[str],            # selected_secretaria
    list[str],            # selected_combustivel
    bool,                 # abrir_editor (flag modal)
]:
    """
    Renderiza toda a sidebar e retorna os valores de filtro selecionados.

    Returns:
        (df_filtrado, data_inicio, data_fim, secretarias, combustiveis, abrir_editor)
    """
    with st.sidebar:
        # ── Filtros ─────────────────────────────────────────────────────────
        data_inicio = data_fim = None
        selected_secretaria: list[str] = []
        selected_combustivel: list[str] = []

        with st.expander("🔎 FILTROS", expanded=True):
            if not df_full.empty and "data" in df_full.columns:
                dmin = pd.to_datetime(df_full["data"]).min().date()
                dmax = pd.to_datetime(df_full["data"]).max().date()

                hoje = date.today()
                default_inicio = max(dmin, date(hoje.year, 1, 1))
                default_fim    = min(dmax, hoje)
                if default_inicio > default_fim:
                    default_inicio, default_fim = dmin, dmax

                col1, col2 = st.columns(2)
                data_inicio = col1.date_input("Data início", value=default_inicio, min_value=dmin, max_value=dmax, key="filter_inicio")
                data_fim    = col2.date_input("Data fim",    value=default_fim,    min_value=dmin, max_value=dmax, key="filter_fim")

                secretarias  = sorted(df_full["secretaria"].dropna().unique().tolist())  if "secretaria"  in df_full.columns else []
                combustiveis = sorted(df_full["combustivel"].dropna().unique().tolist()) if "combustivel" in df_full.columns else []

                selected_secretaria = st.multiselect(
                    "Secretaria", secretarias,
                    default=secretarias,
                    key="filter_secretaria",
                )
                selected_combustivel = st.multiselect(
                    "Combustível", combustiveis,
                    default=combustiveis,
                    key="filter_combustivel",
                )
            else:
                st.info("Importe um arquivo para habilitar os filtros.")

        # ── Parâmetros financeiros ─────────────────────────────────────────
        abrir_editor = False

        # ── Alertas — sempre ano corrente (jan → hoje), independente do filtro ──
        if not df_full.empty:
            from core.services.kpi_service import build_secretaria_status
            from infrastructure.repositories.parametros_repo import get_params
            from core.services.filter_service import apply_filters

            _ano = date.today().year
            _df_ano = apply_filters(df_full, date(_ano, 1, 1), date.today(), [], [])
            df_limits_ano = get_params(settings.db_path, [_ano])
            if not _df_ano.empty and not df_limits_ano.empty:
                status_df = build_secretaria_status(_df_ano, df_limits_ano)
                if not status_df.empty and "status" in status_df.columns:
                    vermelhos = status_df[
                        status_df["status"].isin(["ESTOURO POR PRECO", "ESTOURO GERAL"])
                    ]["secretaria"].tolist()
                    amarelos = status_df[
                        (status_df["status"] == "OK") &
                        (status_df["desvio_pct"] >= -20) &
                        (status_df["desvio_pct"] < 0)
                    ]["secretaria"].tolist()
                    verdes = status_df[
                        (status_df["status"] == "OK") &
                        (status_df["desvio_pct"] < -20)
                    ]["secretaria"].tolist()
                    st.markdown('<div class="sidebar-section">🚦 ALERTAS</div>', unsafe_allow_html=True)
                    _render_alerts({"vermelhos": vermelhos, "amarelos": amarelos, "verdes": verdes})

        # ── Parâmetros financeiros ─────────────────────────────────────────
        st.markdown('<div class="sidebar-section">💰 PARÂMETROS</div>', unsafe_allow_html=True)
        abrir_editor = st.button(
            "Editar Parâmetros Financeiros",
            use_container_width=True,
            key="btn_open_params",
            help="Define empenhos, limites de secretaria e taxa de desconto",
        )

        st.divider()

        # ── Upload ─────────────────────────────────────────────────────────
        with st.expander("📂 Importar relatório Excel", expanded=False):
            uploaded = st.file_uploader(
                "Selecione o arquivo",
                type=["xlsx", "xls"],
                key="sidebar_upload",
                help="Arquivo exportado do sistema Sisatec",
            )
            if uploaded is not None:
                with st.spinner("Importando…"):
                    result = import_excel(uploaded.read(), settings.db_path)
                if result:
                    totals = "  ·  ".join(f"{k}: {v} linhas" for k, v in result.items())
                    st.success(f"Importado — {totals}")
                    st.rerun()
                else:
                    st.error("Falha na importação. Verifique o arquivo.")

        # ── Atualização via API ─────────────────────────────────────────────
        with st.expander("🔄 Atualizar via API Sisatec", expanded=False):
            # Auto-detecta o último timestamp no banco para evitar duplicatas
            _ultimo_ts = None
            if not df_full.empty and "data_hora" in df_full.columns:
                _ts_raw = df_full["data_hora"].max()
                if _ts_raw is not None and not pd.isna(_ts_raw):
                    _ultimo_ts = pd.Timestamp(_ts_raw)

            if _ultimo_ts is not None:
                _hoje = date.today()
                _ultimo_str = _ultimo_ts.strftime("%d/%m/%Y %H:%M")
                st.caption(f"Última importação: **{_ultimo_str}**")
                st.caption(f"Buscará registros após **{_ultimo_str}** até **{_hoje.strftime('%d/%m/%Y')}**")
                if st.button("🌐 Buscar dados da API", key="btn_api_update", use_container_width=True):
                    with st.spinner("Consultando API..."):
                        try:
                            import sqlite3 as _sqlite3
                            client = SisatecClient()
                            registros = client.fetch(_ultimo_ts.date(), _hoje)
                            if not registros:
                                st.info("ℹ️ Nenhum registro novo encontrado no período.")
                            else:
                                _df_api = pd.DataFrame(registros)
                                # Filtra apenas registros POSTERIORES ao último timestamp
                                _df_api["_dt"] = pd.to_datetime(_df_api["Data/Hora"], errors="coerce")
                                _df_api = _df_api[_df_api["_dt"] > _ultimo_ts].drop(columns=["_dt"])
                                if _df_api.empty:
                                    st.info("ℹ️ Nenhum registro novo após o último horário importado.")
                                else:
                                    with _sqlite3.connect(str(settings.db_path)) as _conn_api:
                                        try:
                                            _df_exist = pd.read_sql_query('SELECT * FROM "abastecimentos"', _conn_api)
                                        except Exception:
                                            _df_exist = pd.DataFrame()
                                        # Merge com deduplicação por chave natural
                                        _df_merged = pd.concat(
                                            [_df_exist, _df_api], ignore_index=True
                                        ).drop_duplicates(
                                            subset=["Data/Hora", "Placa", "Qtde (L)", "Valor"],
                                            keep="first",
                                        )
                                        _df_merged.to_sql(
                                            "abastecimentos", _conn_api,
                                            if_exists="replace", index=False,
                                        )
                                    st.session_state["db_version"] = st.session_state.get("db_version", 0) + 1
                                    st.cache_data.clear()
                                    st.success(f"✅ {len(_df_api)} registros novos importados! Recarregando...")
                                    st.rerun()
                        except Exception as exc:
                            st.error(f"Erro na API: {exc}")
            else:
                st.info("ℹ️ Sem dados no banco para determinar o período. Importe um Excel primeiro.")

        # ── Rodapé — mostra a data/hora do último registro no banco ──────────
        if not df_full.empty and "data_hora" in df_full.columns:
            _ts = df_full["data_hora"].max()
            _ultima_str = pd.Timestamp(_ts).strftime("%d/%m/%Y %H:%M") if pd.notna(_ts) else "—"
        else:
            _ultima_str = "—"
        st.markdown(
            f'<div class="sidebar-footer">v2.0 · Última atualização: <b>{_ultima_str}</b></div>',
            unsafe_allow_html=True,
        )

    # Aplica os filtros ao DataFrame completo
    from core.services.filter_service import apply_filters
    df_filtrado = apply_filters(df_full, data_inicio, data_fim, selected_secretaria, selected_combustivel)

    return df_filtrado, data_inicio, data_fim, selected_secretaria, selected_combustivel, abrir_editor
