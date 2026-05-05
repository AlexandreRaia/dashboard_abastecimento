"""
Página de Manutenção — Dashboard de gastos com manutenção de frota.

Arquitetura:
  - Sidebar: filtros de período, secretaria + gestão de empenhos
  - Tab 1 — KPI Cards + gráfico gasto por secretaria
  - Tab 2 — Evolução mensal (linha)
  - Tab 3 — Ranking de veículos
  - Tab 4 — Tabela detalhada
  - Tab 5 — Importar planilha
"""
from __future__ import annotations

import datetime as _dt
import io
from pathlib import Path

_ANO_ATUAL = _dt.date.today().year

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from config import settings
from core.utils.formatters import currency
from infrastructure.database.migrations import ensure_schema
from infrastructure.repositories.manutencao_repo import (
    delete_all_gastos,
    get_parametros_manutencao,
    get_gastos_by_period,
    insert_gastos_df,
    load_gastos,
    save_parametros_manutencao,
    sync_from_gsheets,
)
from ui.components.style_injector import inject_dashboard_style

_MESES = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
}

_PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#c9d6e3", family="Space Grotesk, sans-serif"),
    margin=dict(l=10, r=10, t=36, b=10),
    legend=dict(bgcolor="rgba(0,0,0,0)", borderwidth=0),
)

_COLORS = ["#38bdf8", "#f59e0b", "#4ade80", "#f87171", "#a78bfa",
           "#fb923c", "#34d399", "#e879f9", "#60a5fa", "#fbbf24"]


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

@st.cache_data(show_spinner=False, ttl=120)
def _load(_db: str) -> pd.DataFrame:
    return load_gastos(Path(_db))


@st.cache_data(show_spinner=False, ttl=120)
def _load_params(_db: str) -> pd.DataFrame:
    return get_parametros_manutencao(Path(_db))


# ---------------------------------------------------------------------------
# KPI cards HTML
# ---------------------------------------------------------------------------

def _kpi_html(label: str, value: str, sub: str = "") -> str:
    sub_html = f'<div class="kpi-delta" style="color:#94a3b8;background:rgba(148,163,184,.12);">{sub}</div>' if sub else ""
    return (
        f'<div class="kpi-card">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'{sub_html}'
        f'</div>'
    )


def _render_kpis(df: pd.DataFrame, empenhado: float) -> None:
    total_fat  = float(df["total"].sum()) if not df.empty else 0.0
    vlr_mo     = float(df["vlr_mo"].sum()) if not df.empty else 0.0
    vlr_pecas  = float(df["vlr_pecas"].sum()) if not df.empty else 0.0
    saldo      = empenhado - total_fat

    # Média mensal: dentro de cada ano, divide pelo nº de meses com dados;
    # se há múltiplos anos, usa a média dos totais anuais (não acumula meses).
    if not df.empty:
        anos_no_df = df["ano"].dropna().unique()
        if len(anos_no_df) == 1:
            meses_uniq = df[["ano", "mes"]].drop_duplicates().shape[0]
            media = total_fat / meses_uniq if meses_uniq else 0.0
        else:
            # Média mensal = média dos gastos mensais de cada ano
            medias_anuais = (
                df.groupby(["ano", "mes"])["total"]
                .sum()
                .reset_index()
                .groupby("ano")["total"]
                .mean()
            )
            media = float(medias_anuais.mean()) if not medias_anuais.empty else 0.0
    else:
        media = 0.0

    cobertura  = saldo / media if (media > 0 and empenhado > 0) else 0.0

    # Cards que dependem do empenho: exibe "—" quando não há contrato cadastrado
    _tem_emp = empenhado > 0
    if _tem_emp:
        emp_str = currency(empenhado)
        saldo_str = currency(saldo)
        warn = ' style="color:#f87171;"' if 0 < cobertura < 3 else ""
        cob_str = f'<span{warn}>{cobertura:.1f} {"⚠️" if 0 < cobertura < 3 else "meses"}</span>'
    else:
        emp_str   = '<span style="color:#64748b;">—</span>'
        saldo_str = '<span style="color:#64748b;">—</span>'
        cob_str   = '<span style="color:#64748b;">—</span>'

    html = f"""
    <div class="kpi-grid">
        {_kpi_html("Valor Empenhado", emp_str)}
        {_kpi_html("Gasto Total Faturado", currency(total_fat))}
        {_kpi_html("Saldo Total", saldo_str)}
        {_kpi_html("Média Mensal", currency(media))}
        <div class="kpi-card">
            <div class="kpi-label">Saldo cobre quantos meses</div>
            <div class="kpi-value">{cob_str}</div>
        </div>
    </div>
    <div class="kpi-grid" style="margin-top:8px;">
        {_kpi_html("Total Mão de Obra", currency(vlr_mo))}
        {_kpi_html("Total Peças", currency(vlr_pecas))}
        {_kpi_html("% MO / Total", f"{vlr_mo/total_fat*100:.1f}%" if total_fat else "—")}
        {_kpi_html("% Peças / Total", f"{vlr_pecas/total_fat*100:.1f}%" if total_fat else "—")}
        {_kpi_html("Veículos distintos", str(df["placa"].nunique()) if not df.empty else "0")}
    </div>
    """
    st.markdown("\n".join(l.lstrip() for l in html.splitlines()), unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Gráficos
# ---------------------------------------------------------------------------

def _chart_por_ano(df: pd.DataFrame) -> go.Figure:
    df2 = df.copy()
    df2["ano"] = df2["ano"].astype(str)
    df2["_status_grp"] = df2["status_orcamento"].str.strip().str.upper()

    finalizado = df2[df2["_status_grp"] == "FINALIZADO"]
    andamento  = df2[df2["_status_grp"] != "FINALIZADO"]

    grp_fin = finalizado.groupby("ano")["total"].sum().reset_index()
    grp_and = andamento.groupby("ano")["total"].sum().reset_index()

    # Garante que todos os anos aparecem em ambas as séries
    anos = sorted(df2["ano"].unique())
    grp_fin = grp_fin.set_index("ano").reindex(anos, fill_value=0).reset_index()
    grp_and = grp_and.set_index("ano").reindex(anos, fill_value=0).reset_index()

    total_por_ano = grp_fin["total"].values + grp_and["total"].values

    fig = go.Figure()
    fig.add_bar(
        name="Faturado (Finalizado)",
        x=grp_fin["ano"], y=grp_fin["total"],
        marker_color="#1e40af",
    )
    fig.add_bar(
        name="Em Andamento",
        x=grp_and["ano"], y=grp_and["total"],
        marker_color="#7dd3fc",
        text=[f"R$ {v:,.0f}" for v in total_por_ano],
        textposition="outside",
        textfont=dict(color="#c9d6e3", size=12),
        cliponaxis=False,
    )
    fig.update_layout(
        barmode="stack",
        title="Gasto com Manutenção por Ano",
        margin=dict(l=10, r=10, t=36, b=10, pad=4),
        yaxis=dict(autorange=True),
        legend=dict(bgcolor="rgba(0,0,0,0)", borderwidth=0),
        **{k: v for k, v in _PLOTLY_LAYOUT.items() if k not in ("margin", "legend")},
    )
    fig.update_yaxes(tickprefix="R$ ", tickformat=",.2f")
    fig.update_xaxes(title="Ano", type="category")
    return fig


def _chart_evolucao_mensal(df: pd.DataFrame) -> go.Figure:
    df2 = df.copy()
    df2["periodo"] = df2["ano"].astype(str) + "-" + df2["mes"].astype(str).str.zfill(2)
    grp = df2.groupby("periodo")["total"].sum().reset_index().sort_values("periodo")
    fig = px.bar(grp, x="periodo", y="total",
                 title="Gastos por Mês",
                 color_discrete_sequence=["#38bdf8"])
    fig.update_layout(**_PLOTLY_LAYOUT)
    fig.update_yaxes(tickprefix="R$ ", tickformat=",.2f")
    fig.update_xaxes(title="", type="category")
    return fig


def _chart_top_veiculos(df: pd.DataFrame, n: int = 10) -> go.Figure:
    grp = (
        df.groupby(["placa", "modelo"])["total"]
        .sum()
        .reset_index()
        .sort_values("total", ascending=True)
        .tail(n)
    )
    grp["label"] = grp["placa"] + " — " + grp["modelo"].fillna("")
    fig = px.bar(grp, x="total", y="label", orientation="h",
                 title=f"Top {n} veículos por custo total",
                 color_discrete_sequence=["#f59e0b"])
    fig.update_layout(**_PLOTLY_LAYOUT)
    fig.update_xaxes(tickprefix="R$ ", tickformat=",.2f")
    fig.update_yaxes(title="")
    return fig


def _chart_rosca_combustivel(df: pd.DataFrame) -> go.Figure:
    grp = (
        df.groupby("combustivel")["total"]
        .sum()
        .reset_index()
        .sort_values("total", ascending=False)
    )
    grp = grp[grp["total"] > 0]
    fig = go.Figure(go.Pie(
        labels=grp["combustivel"],
        values=grp["total"],
        hole=0.55,
        textinfo="label+percent",
        textfont=dict(color="#c9d6e3", size=13),
        marker=dict(colors=_COLORS, line=dict(color="rgba(0,0,0,0)", width=0)),
        hovertemplate="<b>%{label}</b><br>R$ %{value:,.2f}<br>%{percent}<extra></extra>",
    ))
    fig.update_layout(
        title="Gastos por Combustível",
        **_PLOTLY_LAYOUT,
    )
    return fig


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def _render_sidebar(df: pd.DataFrame, df_params: pd.DataFrame) -> dict:
    with st.sidebar:
        st.header("🔧 Manutenção")

        anos = sorted(int(a) for a in df["ano"].dropna().unique() if int(a) > 0) if not df.empty else []

        _CCS_CANONICOS = ["SMA", "SME", "SMS", "SMSU", "SMTT"]

        default_anos = [_ANO_ATUAL] if _ANO_ATUAL in anos else (anos[-1:] if anos else [])
        # Define o padrão só na primeira carga OU quando todos os anos
        # salvos deixaram de existir nos dados (ex: após limpar + reimportar).
        # Se o usuário esvaziou manualmente a seleção, respeita a escolha dele.
        if "man_anos" not in st.session_state:
            st.session_state["man_anos"] = default_anos
        elif st.session_state["man_anos"] and not any(
            a in anos for a in st.session_state["man_anos"]
        ):
            st.session_state["man_anos"] = default_anos
        anos_sel = st.multiselect("Ano", anos, key="man_anos")
        ccs_sel  = st.multiselect("C. Custo", _CCS_CANONICOS, default=_CCS_CANONICOS, key="man_ccs")

        st.divider()
        # Gestão de empenhos
        with st.expander("⚙️ Empenhos Contrato", expanded=False):
            st.caption("Valores empenhados por secretaria/contrato")
            if df_params.empty:
                st.info("Nenhum empenho cadastrado.")
            else:
                edited = st.data_editor(
                    df_params[["secretaria", "contrato", "data_inicio",
                                "data_fim", "valor_empenhado"]],
                    use_container_width=True,
                    hide_index=True,
                    num_rows="dynamic",
                    key="man_params_editor",
                    column_config={
                        "secretaria":      st.column_config.TextColumn("Centro de Custo"),
                        "contrato":        st.column_config.TextColumn("Contrato"),
                        "data_inicio":     st.column_config.TextColumn("Início"),
                        "data_fim":        st.column_config.TextColumn("Fim"),
                        "valor_empenhado": st.column_config.NumberColumn(
                            "Empenhado (R$)", format="%.2f", min_value=0),
                    },
                )
            if st.button("💾 Salvar empenhos", key="man_params_save"):
                n = save_parametros_manutencao(
                    settings.db_path,
                    edited if not df_params.empty else pd.DataFrame(),
                )
                _load_params.clear()
                st.success(f"{n} linha(s) salva(s).")
                st.rerun()

    return {
        "anos": anos_sel,
        "ccs":  ccs_sel,
    }


# ---------------------------------------------------------------------------
# Tab Importar
# ---------------------------------------------------------------------------

def _render_importar() -> None:
    import json
    st.subheader("\U0001f4e5 Importar planilha de manutenção")

    # --- Sincronizar do Google Sheets ---
    _config_path = Path(__file__).resolve().parent.parent.parent / "config.json"
    try:
        _cfg = json.loads(_config_path.read_text(encoding="utf-8"))
        _gsheet_url = _cfg.get("google_sheets", {}).get("manutencao_csv_url", "")
    except Exception:
        _gsheet_url = ""

    col_sync, col_del = st.columns([3, 1])
    with col_sync:
        if st.button("\U0001f504 Sincronizar do Google Sheets", type="primary", key="man_gsheet_sync", use_container_width=True):
            if not _gsheet_url:
                st.error("URL do Google Sheets não configurada em config.json.")
            else:
                with st.spinner("Lendo planilha do Google Sheets\u2026"):
                    try:
                        inserted, ignored = sync_from_gsheets(settings.db_path, _gsheet_url)
                        _load.clear()
                        st.success(f"\u2705 {inserted} registro(s) inserido(s). {ignored} duplicata(s) ignorada(s).")
                        st.rerun()
                    except ValueError as e:
                        st.error(str(e))
                    except Exception as e:
                        st.error(f"Erro inesperado: {e}")
    with col_del:
        if st.button("\U0001f5d1\ufe0f Limpar todos os dados", type="secondary", key="man_clear", use_container_width=True):
            n = delete_all_gastos(settings.db_path)
            _load.clear()
            st.warning(f"{n} registro(s) removido(s).")
            st.rerun()

    st.divider()
    st.markdown("**Ou importe um arquivo local:**")

    col_up, _ = st.columns([3, 1])
    with col_up:
        uploaded = st.file_uploader(
            "Selecione o arquivo Excel ou CSV",
            type=["xlsx", "xls", "csv"],
            key="man_upload",
        )

    if uploaded is None:
        st.info(
            "Formato esperado: Nro · ANO · MÊS · N_MÊS · PLACA · MARCA · MODELO · COMBUSTIVEL · "
            "SECRETARIA · DATA ENTRADA · KM ENTRADA · DATA SAIDA · KM SAIDA · QTD DIAS · "
            "RESUMO PROBLEMA · C. CUSTO · ORÇ · HMO · VLR MO · VLR PEÇAS · TOTAL · "
            "VLR INICIAL · DESCONTO · %FIPE · Status Orçamento · STATUS MANUTENÃO · "
            "NF SERVIÇOS · NF PEÇAS · DATA EMISSÃO"
        )
        return

    try:
        if uploaded.name.endswith(".csv"):
            df_raw = pd.read_csv(uploaded, sep=None, engine="python", encoding="utf-8-sig")
        else:
            df_raw = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"Erro ao ler arquivo: {e}")
        return

    st.markdown("**Pré-visualização (primeiras 10 linhas)**")
    st.dataframe(df_raw.head(10), use_container_width=True, hide_index=True)

    if st.button("✅ Confirmar importação", type="primary", key="man_import_btn"):
        with st.spinner("Importando…"):
            try:
                inserted, ignored = insert_gastos_df(settings.db_path, df_raw)
                _load.clear()
                st.success(f"✅ {inserted} registro(s) inserido(s). {ignored} duplicata(s) ignorada(s).")
                st.rerun()
            except ValueError as e:
                st.error(f"Erro de validação: {e}")
            except Exception as e:
                st.error(f"Erro ao importar: {e}")


# ---------------------------------------------------------------------------
# Página principal
# ---------------------------------------------------------------------------

def run_manutencao_page() -> None:
    st.set_page_config(page_title="Manutenção", page_icon="🔧", layout="wide")
    inject_dashboard_style()

    ensure_schema(settings.db_path)

    df_all    = _load(str(settings.db_path))
    df_params = _load_params(str(settings.db_path))

    sel = _render_sidebar(df_all, df_params)

    st.title("🔧 Manutenção de Frota")

    # Aplica filtros
    df = df_all.copy()
    if sel["anos"]:
        df = df[df["ano"].isin(sel["anos"])]
    if sel["ccs"]:
        df = df[df["centro_custo"].isin(sel["ccs"])]

    # Total empenhado: soma dos empenhos cujo período de contrato
    # sobrepõe pelo menos um dos anos selecionados E cujo centro_custo
    # está presente nos dados filtrados.
    anos_sel = sel["anos"]
    if not df.empty and not df_params.empty:
        cc_uniq = df["centro_custo"].dropna().unique()
        emp_df = df_params[df_params["secretaria"].isin(cc_uniq)].copy()
    else:
        emp_df = df_params.copy()

    if anos_sel and not emp_df.empty:
        def _contrato_cobre_ano(row) -> bool:
            """Retorna True se o contrato sobrepõe algum dos anos selecionados."""
            try:
                ini = int(str(row.get("data_inicio", ""))[:4])
                fim = int(str(row.get("data_fim", ""))[:4])
                return any(ini <= a <= fim for a in anos_sel)
            except Exception:
                return True   # inclui se não conseguir parsear
        emp_df = emp_df[emp_df.apply(_contrato_cobre_ano, axis=1)]

    empenhado = float(emp_df["valor_empenhado"].sum()) if not emp_df.empty else 0.0
    _sem_contrato = empenhado == 0.0 and anos_sel and not df.empty

    tab_dash, tab_veic, tab_tabela, tab_import = st.tabs([
        "📊 Dashboard",
        "🚗 Veículos",
        "📋 Tabela",
        "📥 Importar",
    ])

    with tab_dash:
        if df.empty:
            st.info("Nenhum dado disponível. Importe uma planilha na aba **📥 Importar**.")
        else:
            if _sem_contrato:
                st.caption(
                    f"ℹ️ Nenhum contrato cadastrado para "
                    f"{', '.join(str(a) for a in sorted(anos_sel))} — "
                    "Valor Empenhado, Saldo e Cobertura exibem **—**. "
                    "Cadastre em ⚙️ Empenhos Contrato na sidebar."
                )
            _render_kpis(df, empenhado)
            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                st.plotly_chart(_chart_por_ano(df_all),
                                use_container_width=True)
            with col2:
                st.plotly_chart(_chart_rosca_combustivel(df),
                                use_container_width=True)
            st.plotly_chart(_chart_evolucao_mensal(df), use_container_width=True)

    with tab_veic:
        if df.empty:
            st.info("Sem dados.")
        else:
            n_top = st.slider("Número de veículos", 5, 30, 10, key="man_top_n")
            st.plotly_chart(_chart_top_veiculos(df, n_top), use_container_width=True)

            # Tabela por veículo
            vei = (
                df.groupby(["placa", "marca", "modelo", "secretaria"])
                .agg(
                    meses=("mes", "nunique"),
                    vlr_mo=("vlr_mo", "sum"),
                    vlr_pecas=("vlr_pecas", "sum"),
                    total=("total", "sum"),
                )
                .reset_index()
                .sort_values("total", ascending=False)
            )
            st.dataframe(
                vei,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "placa":     st.column_config.TextColumn("Placa"),
                    "marca":     st.column_config.TextColumn("Marca"),
                    "modelo":    st.column_config.TextColumn("Modelo"),
                    "secretaria":st.column_config.TextColumn("Secretaria"),
                    "meses":     st.column_config.NumberColumn("Meses", format="%d"),
                    "vlr_mo":    st.column_config.NumberColumn("MO (R$)", format="R$ %.2f"),
                    "vlr_pecas": st.column_config.NumberColumn("Peças (R$)", format="R$ %.2f"),
                    "total":     st.column_config.NumberColumn("Total (R$)", format="R$ %.2f"),
                },
            )

    with tab_tabela:
        if df.empty:
            st.info("Sem dados.")
        else:
            # ── Resumo por ano (base COMPLETA, não filtrada por ano) ─────────
            with st.expander("📊 Totais por Ano — base completa", expanded=False):
                resumo_ano = (
                    df_all.groupby("ano")
                    .agg(
                        registros=("total", "count"),
                        vlr_mo=("vlr_mo", "sum"),
                        vlr_pecas=("vlr_pecas", "sum"),
                        total=("total", "sum"),
                    )
                    .reset_index()
                    .sort_values("ano")
                )
                resumo_ano["ano"] = resumo_ano["ano"].astype(int)
                st.dataframe(
                    resumo_ano,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "ano":       st.column_config.NumberColumn("Ano",        format="%d"),
                        "registros": st.column_config.NumberColumn("Registros",  format="%d"),
                        "vlr_mo":    st.column_config.NumberColumn("MO (R$)",    format="R$ %.2f"),
                        "vlr_pecas": st.column_config.NumberColumn("Peças (R$)", format="R$ %.2f"),
                        "total":     st.column_config.NumberColumn("Total (R$)", format="R$ %.2f"),
                    },
                )
            st.divider()
            search = st.text_input("🔍 Buscar placa / modelo / secretaria / problema", key="man_search")
            df_show = df.copy()
            if search:
                cols_search = [c for c in ("placa", "modelo", "secretaria", "centro_custo",
                                           "resumo_problema", "status_manutencao") if c in df_show.columns]
                mask = pd.Series(False, index=df_show.index)
                for c in cols_search:
                    mask |= df_show[c].astype(str).str.contains(search, case=False, na=False)
                df_show = df_show[mask]
            st.caption(f"{len(df_show)} registro(s)")
            _COLS = [
                "nro", "ano", "mes", "n_mes", "placa", "marca", "modelo", "combustivel",
                "secretaria", "centro_custo", "data_entrada", "km_entrada",
                "data_saida", "km_saida", "qtd_dias", "resumo_problema",
                "orc", "hmo", "vlr_mo", "vlr_pecas", "total",
                "vlr_inicial", "desconto", "pct_fipe",
                "status_orcamento", "status_manutencao",
                "nf_servicos", "nf_pecas", "data_emissao",
            ]
            st.dataframe(
                df_show[[c for c in _COLS if c in df_show.columns]].reset_index(drop=True),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "nro":              st.column_config.NumberColumn("Nro",       format="%d"),
                    "ano":              st.column_config.NumberColumn("Ano",       format="%d"),
                    "mes":              st.column_config.NumberColumn("Mês",       format="%d"),
                    "n_mes":            st.column_config.TextColumn("N Mês"),
                    "placa":            st.column_config.TextColumn("Placa"),
                    "marca":            st.column_config.TextColumn("Marca"),
                    "modelo":           st.column_config.TextColumn("Modelo"),
                    "combustivel":      st.column_config.TextColumn("Combustível"),
                    "secretaria":       st.column_config.TextColumn("Secretaria"),
                    "centro_custo":     st.column_config.TextColumn("C. Custo"),
                    "data_entrada":     st.column_config.TextColumn("Entrada"),
                    "km_entrada":       st.column_config.NumberColumn("KM Entrada", format="%.0f"),
                    "data_saida":       st.column_config.TextColumn("Saída"),
                    "km_saida":         st.column_config.NumberColumn("KM Saída",   format="%.0f"),
                    "qtd_dias":         st.column_config.NumberColumn("Dias",       format="%d"),
                    "resumo_problema":  st.column_config.TextColumn("Problema"),
                    "orc":              st.column_config.TextColumn("Orç."),
                    "hmo":              st.column_config.NumberColumn("HMO",        format="%.1f"),
                    "vlr_mo":           st.column_config.NumberColumn("MO (R$)",    format="R$ %.2f"),
                    "vlr_pecas":        st.column_config.NumberColumn("Peças (R$)", format="R$ %.2f"),
                    "total":            st.column_config.NumberColumn("Total (R$)", format="R$ %.2f"),
                    "vlr_inicial":      st.column_config.NumberColumn("Vlr Inicial",format="R$ %.2f"),
                    "desconto":         st.column_config.NumberColumn("Desconto",   format="R$ %.2f"),
                    "pct_fipe":         st.column_config.NumberColumn("%FIPE",      format="%.2f"),
                    "status_orcamento": st.column_config.TextColumn("Status Orç."),
                    "status_manutencao":st.column_config.TextColumn("Status Manut."),
                    "nf_servicos":      st.column_config.TextColumn("NF Serv."),
                    "nf_pecas":         st.column_config.TextColumn("NF Peças"),
                    "data_emissao":     st.column_config.TextColumn("Emissão"),
                },
            )

            # Exportar CSV
            csv_buf = io.StringIO()
            df_show.to_csv(csv_buf, index=False, encoding="utf-8-sig")
            st.download_button(
                "⬇️ Exportar CSV",
                data=csv_buf.getvalue().encode("utf-8-sig"),
                file_name="manutencao_filtrado.csv",
                mime="text/csv",
                key="man_csv_dl",
            )

    with tab_import:
        _render_importar()

