"""
Página principal do Dashboard de Abastecimento.

Orquestra o carregamento de dados, sidebar, KPI cards e todos os gráficos.
Esta página não contém lógica de negócio — apenas composição dos componentes.
"""
from __future__ import annotations

import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from config import settings
from core.services.discount_service import apply_discount
from core.services.filter_service import apply_filters
from core.services.kpi_service import (
    build_kpis,
    build_secretaria_status,
    month_count,
)
from core.utils.date_utils import date_range_years
from core.utils.formatters import currency
from infrastructure.database.migrations import ensure_schema
from infrastructure.repositories.abastecimento_repo import load_abastecimentos
from infrastructure.repositories.parametros_repo import get_all_params, get_params, save_params
from ui.components.charts import (
    apply_plotly_theme,
    make_bar_comparativo_mensal_yoy,
    make_bar_comparativo_mensal_yoy_litros,
    make_bar_consumo_secretaria,
    make_bar_consumo_tipo_mes,
    make_bar_consumo_tipo_mes_litros,
    make_bar_gasto_por_ano,
    make_bar_gasto_por_mes_unificado,
    make_bar_litros_vs_limite_secretaria,
    make_bar_valor_vs_limite_secretaria,
    make_donut_combustivel,
    make_donut_combustivel_valor,
    make_line_custo_medio_mes_combustivel,
    make_line_custo_medio_rl_combustivel,
    make_line_real_previsto_projecao,
    make_line_sazonalidade_yoy,
    make_ranking_consumo_secretaria,
    make_ranking_veiculos,
    make_treemap_postos,
)
from ui.components.kpi_cards import render_kpi_cards
from ui.components.sidebar import render_sidebar
from ui.components.style_injector import inject_dashboard_style


# ---------------------------------------------------------------------------
# Cache de dados
# ---------------------------------------------------------------------------

@st.cache_data(show_spinner=False, ttl=300)
def _load_data(db_path: str, db_version: int) -> pd.DataFrame:
    """
    Carrega e normaliza os dados do banco SQLite.

    Parâmetro `db_version` serve como cache-buster: incrementado após imports,
    força recarregamento sem depender de TTL.
    """
    _ = db_version  # usado apenas como chave de cache
    df = load_abastecimentos(db_path)
    # Garante coluna 'data' como alias de 'data_hora' para compatibilidade com UI
    if "data_hora" in df.columns and "data" not in df.columns:
        df["data"] = df["data_hora"]
    return df


# ---------------------------------------------------------------------------
# Modal de parâmetros financeiros
# ---------------------------------------------------------------------------

@st.dialog("⚙️ Parâmetros Financeiros Anuais", width="large")
def _render_params_modal() -> None:
    """Modal de edição de parâmetros financeiros (empenhos, limites, desconto)."""
    df_params = get_all_params(settings.db_path)

    anos_disponiveis = sorted(df_params["ano"].dropna().unique().tolist()) if not df_params.empty else []
    ano_sel = st.selectbox("Filtrar por ano", ["Todos"] + [str(a) for a in anos_disponiveis], key="params_modal_ano")

    if ano_sel == "Todos":
        editor_df = df_params.copy()
    else:
        editor_df = df_params[df_params["ano"] == int(ano_sel)].copy()

    st.caption("Edite os valores, adicione ou remova linhas. Clique em **Salvar** para aplicar.")
    edited = st.data_editor(
        editor_df,
        use_container_width=True,
        height=480,
        hide_index=True,
        num_rows="dynamic",
        key=f"params_editor_{ano_sel}",
        column_config={
            "secretaria":               st.column_config.TextColumn("Secretaria", required=True),
            "ano":                      st.column_config.NumberColumn("Ano", min_value=2000, max_value=2100, step=1),
            "valor_empenhado":          st.column_config.NumberColumn("Valor empenhado (R$)", min_value=0.0, format="%.2f"),
            "limite_litros_gasolina":   st.column_config.NumberColumn("Limite gasolina (L)", min_value=0.0, format="%.2f"),
            "limite_litros_alcool":     st.column_config.NumberColumn("Limite álcool (L)", min_value=0.0, format="%.2f"),
            "limite_litros_diesel":     st.column_config.NumberColumn("Limite diesel (L)", min_value=0.0, format="%.2f"),
            "desconto_pct":             st.column_config.NumberColumn("Desconto (%)", min_value=0.0, max_value=100.0, format="%.2f"),
        },
    )

    col_save, col_close = st.columns(2)
    if col_save.button("💾 Salvar", use_container_width=True, type="primary", key="params_save_btn"):
        try:
            if ano_sel == "Todos":
                df_to_save = edited.copy()
            else:
                df_outros = df_params[df_params["ano"] != int(ano_sel)].copy()
                df_to_save = pd.concat([df_outros, edited], ignore_index=True)
            saved, deleted = save_params(settings.db_path, df_to_save)
        except Exception as exc:
            st.error(f"Não foi possível salvar: {exc}")
        else:
            _load_data.clear()
            st.session_state["db_version"] = st.session_state.get("db_version", 0) + 1
            st.toast(f"Parâmetros salvos. Linhas ativas: {saved}. Removidas: {deleted}.")
            st.rerun()

    if col_close.button("Fechar", use_container_width=True, key="params_close_btn"):
        st.rerun()


# ---------------------------------------------------------------------------
# Cálculo de deltas YoY
# ---------------------------------------------------------------------------

def _calc_yoy_deltas(
    df_real: pd.DataFrame,
    kpis: dict,
    data_inicio: datetime.date | None,
    data_fim: datetime.date | None,
    selected_secretaria: list[str],
    selected_combustivel: list[str],
    df_limits: pd.DataFrame,
) -> dict | None:
    """
    Calcula variações Year-over-Year para exibição nos KPI cards.

    Retorna None se não houver período de referência definido.
    """
    if not (data_inicio and data_fim):
        return None

    try:
        yoy_ini = data_inicio.replace(year=data_inicio.year - 1)
        yoy_fim = data_fim.replace(year=data_fim.year - 1)
    except ValueError:
        yoy_ini = data_inicio.replace(year=data_inicio.year - 1, day=28)
        yoy_fim = data_fim.replace(year=data_fim.year - 1, day=28)

    filtered_yoy = apply_filters(df_real, yoy_ini, yoy_fim, selected_secretaria, selected_combustivel)
    if filtered_yoy.empty:
        return None

    def _pct(cur: float, prev: float) -> float | None:
        return (cur - prev) / prev * 100 if prev else None

    ano_ref = yoy_ini.year
    gasto_yoy    = float(filtered_yoy["valor"].sum())
    litros_yoy   = float(filtered_yoy["litros"].sum()) if "litros" in filtered_yoy.columns else 0.0
    n_yoy        = len(filtered_yoy)
    months_yoy   = month_count(filtered_yoy)
    media_yoy    = gasto_yoy / months_yoy if months_yoy else 0.0
    consumo_yoy  = float(filtered_yoy["km_por_litro"].mean()) if "km_por_litro" in filtered_yoy.columns and filtered_yoy["km_por_litro"].notna().any() else 0.0
    custo_km_yoy = float(filtered_yoy["custo_por_km"].mean()) if "custo_por_km" in filtered_yoy.columns and filtered_yoy["custo_por_km"].notna().any() else 0.0
    veiculos_yoy = filtered_yoy["placa"].nunique() if "placa" in filtered_yoy.columns else 0

    anos_prev = date_range_years(yoy_ini, yoy_fim)
    limits_prev = get_params(settings.db_path, anos_prev)
    empenhado_yoy = float(limits_prev["valor_empenhado"].sum()) if not limits_prev.empty else 0.0

    deltas: dict = {}
    if empenhado_yoy:
        deltas["valor_empenhado"] = (_pct(kpis["valor_empenhado"], empenhado_yoy), ano_ref)
    deltas["gasto_total"]           = (_pct(kpis["gasto_total"],           gasto_yoy),    ano_ref)
    if litros_yoy:
        deltas["gasto_litros"]      = (_pct(kpis["gasto_litros"],          litros_yoy),   ano_ref)
    deltas["n_abastecimentos"]      = (_pct(kpis["n_abastecimentos"],      n_yoy),         ano_ref)
    if media_yoy:
        deltas["media_mensal_consumo"] = (_pct(kpis["media_mensal_consumo"], media_yoy),  ano_ref)
    if consumo_yoy:
        deltas["consumo_medio"]     = (_pct(kpis["consumo_medio"],         consumo_yoy),  ano_ref)
    if custo_km_yoy:
        deltas["custo_por_km"]      = (_pct(kpis["custo_por_km"],          custo_km_yoy), ano_ref)
    if veiculos_yoy:
        deltas["veiculos_ativos"]   = (_pct(kpis["veiculos_ativos"],       veiculos_yoy), ano_ref)

    return deltas or None


# ---------------------------------------------------------------------------
# Ponto de entrada principal
# ---------------------------------------------------------------------------

def run_dashboard() -> None:
    """
    Ponto de entrada do dashboard.

    Fluxo:
    1. Configuração da página Streamlit
    2. Injeção de estilos
    3. Garantia de schema do banco
    4. Carregamento dos dados com cache
    5. Renderização da sidebar (filtros, upload, alertas)
    6. KPI cards
    7. Abas com gráficos
    """
    # ── Configuração da página ──────────────────────────────────────────────
    st.set_page_config(
        page_title="Painel de Abastecimento",
        page_icon="⛽",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_dashboard_style()

    # ── Schema do banco ─────────────────────────────────────────────────────
    ensure_schema(settings.db_path)

    # ── Estado de sessão ────────────────────────────────────────────────────
    if "db_version" not in st.session_state:
        st.session_state["db_version"] = 0


    # ── Carregamento dos dados ──────────────────────────────────────────────
    try:
        df_raw = _load_data(str(settings.db_path), st.session_state["db_version"])
    except RuntimeError as exc:
        if "Nenhuma tabela" in str(exc):
            st.error(
                "⚠️ **Banco de dados vazio.**\n\n"
                "Use o botão **📂 Importar relatório Excel** na barra lateral para carregar os dados."
            )
        else:
            st.error(f"Erro ao carregar dados: {exc}")
        # Renderiza apenas o uploader para permitir importação sem dados
        with st.sidebar:
            from ui.components.sidebar import render_sidebar as _rsb
        render_sidebar(pd.DataFrame())
        st.stop()

    # Aplica desconto global (taxa vem dos parâmetros financeiros)
    discount_rate = settings.default_discount_rate
    df_real = apply_discount(df_raw, discount_rate)

    # ── Sidebar ─────────────────────────────────────────────────────────────
    (
        df_filtered,
        data_inicio,
        data_fim,
        selected_secretaria,
        selected_combustivel,
        abrir_editor,
    ) = render_sidebar(df_real)

    if abrir_editor:
        _render_params_modal()

    # ── Scopes para gráficos ────────────────────────────────────────────────
    # anual_scope: todos os anos, sem filtro de período (para comparativo YoY anual)
    anual_scope = apply_filters(df_real, None, None, selected_secretaria, selected_combustivel)
    # yoy_scope: mesmos filtros de secretaria/combustível mas sem corte de período
    yoy_scope = anual_scope

    # Limites financeiros do período selecionado
    anos_periodo = date_range_years(data_inicio, data_fim) if (data_inicio and data_fim) else []
    if not anos_periodo and not df_real.empty and "data" in df_real.columns:
        dmin = pd.to_datetime(df_real["data"]).min().date()
        dmax = pd.to_datetime(df_real["data"]).max().date()
        anos_periodo = date_range_years(dmin, dmax)

    df_limits = get_params(settings.db_path, anos_periodo)

    # Filtra limites pela secretaria selecionada (se não for "todas")
    limits_scope = df_limits.copy()
    if selected_secretaria and len(selected_secretaria) < len(
        df_real["secretaria"].dropna().unique()
    ) if "secretaria" in df_real.columns else False:
        limits_scope = limits_scope[limits_scope["secretaria"].isin(selected_secretaria)]

    # ── KPIs e status de secretarias ───────────────────────────────────────
    status = build_secretaria_status(df_filtered, limits_scope)
    kpis   = build_kpis(df_filtered, status, limits_scope)

    # ── Cabeçalho ───────────────────────────────────────────────────────────
    if data_inicio and data_fim:
        filtro_ctx = f"{data_inicio.strftime('%d/%m/%Y')} → {data_fim.strftime('%d/%m/%Y')}"
    else:
        filtro_ctx = "Todo o período"

    n_secs = len(selected_secretaria) if selected_secretaria else 0
    total_secs = len(df_real["secretaria"].dropna().unique()) if "secretaria" in df_real.columns else 0
    if selected_secretaria and n_secs < total_secs:
        filtro_ctx += f" · {', '.join(selected_secretaria[:2])}" + ("…" if n_secs > 2 else "")
    if selected_combustivel and len(selected_combustivel) < len(
        df_real["combustivel"].dropna().unique() if "combustivel" in df_real.columns else []
    ):
        filtro_ctx += f" · {', '.join(selected_combustivel)}"

    st.markdown(
        f"""
        <div style="display:flex;align-items:baseline;gap:16px;margin-bottom:0.5rem;
                    padding-bottom:0.4rem;border-bottom:1px solid rgba(142,163,190,0.18);">
            <span style="font-family:'Rajdhani',sans-serif;font-size:2rem;font-weight:700;
                         color:#e7eef8;letter-spacing:0.02em;">DASHBOARD DE ABASTECIMENTO</span>
            <span style="font-size:0.88rem;color:#8ea3be;font-family:'Space Grotesk',sans-serif;">
                Análise completa da frota &nbsp;•&nbsp; {filtro_ctx}
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── KPI Cards ───────────────────────────────────────────────────────────
    yoy_deltas = _calc_yoy_deltas(
        df_real, kpis, data_inicio, data_fim,
        selected_secretaria, selected_combustivel, df_limits,
    )
    render_kpi_cards(kpis, deltas=yoy_deltas)
    st.caption(f"Valores com desconto contratual de {discount_rate * 100:.2f}% aplicado sobre o valor bruto.")

    # ── Abas ────────────────────────────────────────────────────────────────
    tab_fin, tab_con, tab_sec, tab_veic, tab_postos = st.tabs(
        ["📊 Financeiro", "⛽ Consumo", "🏢 Secretarias", "🚗 Veículos", "⛽ Postos"]
    )

    with tab_fin:
        col_ano, col_mes = st.columns([1, 2])
        col_ano.plotly_chart(
            make_bar_gasto_por_ano(anual_scope, discount_rate),
            use_container_width=True, key="bar_gasto_ano",
        )
        # Meta mensal = empenho total / 12
        meta_mensal = float(limits_scope["valor_empenhado"].sum()) / 12.0 if not limits_scope.empty else 0.0
        col_mes.plotly_chart(
            make_bar_gasto_por_mes_unificado(df_filtered, meta_mensal),
            use_container_width=True, key="bar_gasto_mes_unificado",
        )
        st.plotly_chart(
            make_bar_comparativo_mensal_yoy(yoy_scope, data_inicio, data_fim),
            use_container_width=True, key="bar_comparativo_mensal_yoy",
        )
        bar_col, donut_col = st.columns([2, 1])
        bar_col.plotly_chart(make_bar_consumo_tipo_mes(df_filtered), use_container_width=True, key="bar_combustivel_fin")
        donut_col.plotly_chart(make_donut_combustivel_valor(df_filtered), use_container_width=True, key="donut_combustivel_valor")
        st.plotly_chart(
            make_line_real_previsto_projecao(df_filtered, limits_scope),
            use_container_width=True, key="line_real_previsto",
        )
        st.plotly_chart(make_line_custo_medio_rl_combustivel(df_filtered), use_container_width=True, key="line_custo_medio_rl")
        st.plotly_chart(make_bar_valor_vs_limite_secretaria(status), use_container_width=True, key="bar_valor_limite_sec")

    with tab_con:
        bar_con_col, donut_con_col = st.columns([2, 1])
        bar_con_col.plotly_chart(make_bar_consumo_tipo_mes_litros(df_filtered), use_container_width=True, key="bar_combustivel_litros")
        donut_con_col.plotly_chart(make_donut_combustivel(df_filtered), use_container_width=True, key="donut_combustivel")
        st.plotly_chart(
            make_bar_comparativo_mensal_yoy_litros(yoy_scope, data_inicio, data_fim),
            use_container_width=True, key="bar_comp_mensal_litros",
        )
        st.plotly_chart(make_line_custo_medio_mes_combustivel(df_filtered), use_container_width=True, key="line_custo_medio")
        st.plotly_chart(make_line_sazonalidade_yoy(yoy_scope), use_container_width=True, key="line_sazonalidade_yoy")

    with tab_sec:
        st.plotly_chart(make_ranking_consumo_secretaria(status), use_container_width=True, key="bar_ranking_sec")
        st.plotly_chart(make_bar_consumo_secretaria(status, df_limits), use_container_width=True, key="bar_sec")
        if not df_limits.empty:
            st.plotly_chart(
                make_bar_litros_vs_limite_secretaria(df_filtered, df_limits),
                use_container_width=True, key="bar_litros_limite_sec",
            )
        # Tabela de secretarias excedidas
        if "status" in status.columns:
            excedidas = status[status["status"].isin(["ESTOURO POR PRECO", "ESTOURO GERAL"])]
            if not excedidas.empty:
                st.markdown("#### 🔴 Secretarias com Limite Excedido")
                cols_show = [c for c in ("secretaria", "gasto_valor", "limite_valor_periodo", "desvio_pct", "desvio_valor") if c in excedidas.columns]
                st.dataframe(
                    excedidas[cols_show].rename(columns={
                        "secretaria": "Secretaria",
                        "gasto_valor": "Gasto (R$)",
                        "limite_valor_periodo": "Limite (R$)",
                        "desvio_pct": "Desvio (%)",
                        "desvio_valor": "Desvio (R$)",
                    }),
                    use_container_width=True,
                )

    with tab_veic:
        n_top = st.slider("Número de veículos no ranking", 5, 50, 20, key="slider_top_n")
        st.plotly_chart(make_ranking_veiculos(df_filtered, top_n=n_top), use_container_width=True, key="ranking_veic_valor")

    with tab_postos:
        st.plotly_chart(make_treemap_postos(df_filtered), use_container_width=True, key="treemap_postos")
        if not df_filtered.empty and "posto" in df_filtered.columns:
            df_postos_tbl = (
                df_filtered.groupby("posto", as_index=False)
                .agg(valor=("valor", "sum"), litros=("litros", "sum"), abastecimentos=("placa", "count"))
                .sort_values("valor", ascending=False)
                .rename(columns={"posto": "Posto", "valor": "Gasto (R$)", "litros": "Litros", "abastecimentos": "Abastecimentos"})
            )
            st.dataframe(df_postos_tbl.reset_index(drop=True), use_container_width=True)
