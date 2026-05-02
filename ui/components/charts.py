"""
Módulo de gráficos Plotly para o dashboard de abastecimento.

Consolida todas as funções make_*() em um único lugar, eliminando
duplicação entre Abastecimento.py, make_bar_consumo_secretaria.py e
bar_consumo_combustivel.py.

Todas as funções:
- Recebem DataFrames já filtrados
- Retornam go.Figure (nunca chamam st.plotly_chart)
- Aplicam apply_plotly_theme() antes de retornar
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from config.constants import MONTHS, MONTHS_SHORT, FUEL_COLORS, YEAR_PALETTE
from core.services.kpi_service import build_monthly_mix, month_count
from core.utils.formatters import currency

# ---------------------------------------------------------------------------
# Tema Plotly — única definição no projeto
# ---------------------------------------------------------------------------

CHART_TITLE_FONT = {
    "family": "'Rajdhani', 'Space Grotesk', sans-serif",
    "size": 16,
    "color": "#e7eef8",
}

_THEME = {"panel": "#101926", "text": "#eaf2ff"}


def apply_plotly_theme(fig: go.Figure) -> go.Figure:
    """
    Aplica o tema escuro padrão a qualquer figura Plotly.

    Deve ser chamado ao final de cada função make_*() como último passo.
    """
    fig.update_layout(
        paper_bgcolor=_THEME["panel"],
        plot_bgcolor=_THEME["panel"],
        font={
            "color": _THEME["text"],
            "family": "'Space Grotesk', sans-serif",
            "size": 13,
        },
        legend={
            "font": {"size": 13, "color": "#e8f1ff"},
            "bgcolor": "rgba(8, 17, 28, 0.65)",
        },
        hoverlabel={
            "bgcolor": "#0b1626",
            "bordercolor": "#38bdf8",
            "font": {"color": "#f8fbff", "size": 13},
        },
        separators=",.",
    )
    if fig.layout.title and fig.layout.title.text:
        fig.update_layout(
            title={
                "font": CHART_TITLE_FONT,
                "x": 0.01,
                "xanchor": "left",
                "pad": {"t": 8},
            }
        )
    fig.update_xaxes(gridcolor="rgba(142,163,190,0.20)", tickfont={"size": 13})
    fig.update_yaxes(gridcolor="rgba(142,163,190,0.20)", tickfont={"size": 13})
    return fig


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _corrige_mes(nome: str) -> str:
    """Corrige 'marco' → 'Março' em labels de período."""
    return "Março" if str(nome).strip().lower() == "marco" else nome


def _bar_color_pct(pct: float) -> str:
    """Retorna cor semafórica baseada em percentual do limite."""
    if pct > 100:
        return "#ef4444"   # vermelho — excedido
    if pct > 80:
        return "#eab308"   # amarelo — atenção
    return "#22c55e"       # verde — ok


# ---------------------------------------------------------------------------
# Gráfico 1: Consumo mensal unificado (barra empilhada + meta)
# ---------------------------------------------------------------------------

def make_bar_gasto_por_mes_unificado(
    df_filtered: pd.DataFrame,
    meta_mensal: float = 0.0,
) -> go.Figure:
    """
    Barras mensais empilhadas (consumo até meta + excesso) com linha de meta.

    Args:
        df_filtered: DataFrame filtrado por período/secretaria/combustível.
        meta_mensal: Meta mensal em R$ (calculada externamente pelo dashboard).
    """
    _, monthly_totals = build_monthly_mix(df_filtered)
    fig = go.Figure()

    if monthly_totals.empty:
        fig.update_layout(template="plotly_dark", title="Consumo por mês — sem dados")
        return apply_plotly_theme(fig)

    agrupado = monthly_totals.groupby(
        ["ano", "mes", "periodo_corrigido"], as_index=False
    ).agg({"valor_total_mes": "sum", "litros_total_mes": "sum", "variacao_pct": "first"})

    if meta_mensal > 0:
        agrupado["azul"] = agrupado["valor_total_mes"].clip(upper=meta_mensal)
        agrupado["vermelho"] = (agrupado["valor_total_mes"] - meta_mensal).clip(lower=0)
    else:
        agrupado["azul"] = agrupado["valor_total_mes"]
        agrupado["vermelho"] = 0.0

    customdata = list(
        zip(
            agrupado["variacao_pct"],
            agrupado["litros_total_mes"],
            [meta_mensal] * len(agrupado),
            agrupado["vermelho"],
        )
    )

    fig.add_trace(
        go.Bar(
            x=agrupado["periodo_corrigido"],
            y=agrupado["azul"],
            name="Consumo até a meta",
            marker={"color": "#2563eb", "line": {"color": "#102a56", "width": 1.5}},
            customdata=customdata,
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Variação mensal: %{customdata[0]:+.2f}%<br>"
                "Litros: %{customdata[1]:,.0f}<br>"
                "Meta mensal: R$ %{customdata[2]:,.2f}<extra></extra>"
            ),
        )
    )
    fig.add_trace(
        go.Bar(
            x=agrupado["periodo_corrigido"],
            y=agrupado["vermelho"],
            name="Excesso sobre a meta",
            marker={"color": "#e63946", "line": {"color": "#102a56", "width": 1.5}},
            customdata=customdata,
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Excedeu: %{y:,.2f}<br>"
                "Variação: %{customdata[0]:+.2f}%<br>"
                "Meta: R$ %{customdata[2]:,.2f}<extra></extra>"
            ),
        )
    )
    # Rótulo com valor total no topo de cada barra
    fig.add_trace(
        go.Scatter(
            x=agrupado["periodo_corrigido"],
            y=agrupado["valor_total_mes"].tolist(),
            mode="text",
            text=[currency(t) for t in agrupado["valor_total_mes"]],
            textposition="top center",
            showlegend=False,
            textfont={"size": 12, "color": "#fff", "family": "'Space Grotesk', sans-serif"},
            hoverinfo="skip",
        )
    )

    n_bars = len(agrupado)
    media = float(agrupado["valor_total_mes"].mean()) if not agrupado.empty else 0.0

    if meta_mensal > 0:
        fig.add_shape(type="line", x0=-0.5, x1=n_bars - 0.5, y0=meta_mensal, y1=meta_mensal,
                      line=dict(color="#ef4444", width=3, dash="dash"), xref="x", yref="y", layer="above")
        fig.add_trace(go.Scatter(x=[None], y=[None], mode="lines",
                                 line=dict(color="#ef4444", width=3, dash="dash"),
                                 name=f"Limite mensal ({currency(meta_mensal)})"))

    if media > 0:
        fig.add_shape(type="line", x0=-0.5, x1=n_bars - 0.5, y0=media, y1=media,
                      line=dict(color="#eab308", width=2, dash="dot"), xref="x", yref="y", layer="above")
        fig.add_trace(go.Scatter(x=[None], y=[None], mode="lines",
                                 line=dict(color="#eab308", width=2, dash="dot"),
                                 name=f"Média ({currency(media)})"))

    fig.update_layout(
        template="plotly_dark", title="Consumo Combustível por mês",
        xaxis_title="Período", yaxis_title="Valor faturado",
        margin={"l": 30, "r": 30, "t": 48, "b": 60},
        bargap=0.45, barmode="stack",
        legend={"orientation": "h", "x": 0.5, "y": -0.18, "xanchor": "center", "yanchor": "top"},
    )
    return apply_plotly_theme(fig)


# ---------------------------------------------------------------------------
# Gráfico 2: Gasto anual comparativo
# ---------------------------------------------------------------------------

def make_bar_gasto_por_ano(
    df: pd.DataFrame,
    discount_rate: float = 0.0,
) -> go.Figure:
    """Barras por ano com informação de desconto no hover."""
    if df.empty or not {"ano", "valor"}.issubset(df.columns):
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados para gasto anual")
        return apply_plotly_theme(fig)

    grupo = df.groupby("ano", as_index=False).agg(valor_total=("valor", "sum")).sort_values("ano")
    grupo["valor_bruto"] = grupo["valor_total"] / (1 - discount_rate) if discount_rate > 0 else grupo["valor_total"]
    grupo["valor_desconto"] = grupo["valor_bruto"] - grupo["valor_total"]

    fig = go.Figure(go.Bar(
        x=grupo["ano"].astype(str),
        y=grupo["valor_total"],
        marker_color=[YEAR_PALETTE.get(int(a), "#60a5fa") for a in grupo["ano"]],
        text=[currency(v) for v in grupo["valor_total"]],
        textposition="outside",
        textfont={"size": 16, "color": "#fff", "family": "'Space Grotesk', sans-serif"},
        name="Gasto anual",
        customdata=grupo[["valor_bruto", "valor_desconto"]],
        hovertemplate=(
            "<b>Ano %{x}</b><br>"
            "Valor bruto: <b>R$ %{customdata[0]:,.2f}</b><br>"
            f"Desconto (-{discount_rate*100:.1f}%): <b>-R$ %{{customdata[1]:,.2f}}</b><br>"
            "Valor pago: <b>R$ %{y:,.2f}</b><extra></extra>"
        ),
    ))
    fig.update_layout(
        template="plotly_dark", title="Gasto anual comparativo",
        xaxis_title="Ano", yaxis_title="Valor total (R$)",
        margin={"l": 20, "r": 20, "t": 48, "b": 30},
    )
    return apply_plotly_theme(fig)


# ---------------------------------------------------------------------------
# Gráficos de donut por combustível
# ---------------------------------------------------------------------------

def make_donut_combustivel(df_filtered: pd.DataFrame) -> go.Figure:
    """Donut: volume em litros por tipo de combustível."""
    if df_filtered.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados por combustível")
        return apply_plotly_theme(fig)

    by_fuel = df_filtered.groupby("combustivel", as_index=False).agg(litros=("litros", "sum"))
    fig = go.Figure(go.Pie(
        labels=by_fuel["combustivel"], values=by_fuel["litros"], hole=0.5,
        marker_colors=[FUEL_COLORS.get(c, "#2563eb") for c in by_fuel["combustivel"]],
        textinfo="label+percent",
        hovertemplate="<b>%{label}</b><br>%{value:,.0f} L<br>%{percent}<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_dark", title="Combustível por volume (Litros)",
        margin={"l": 20, "r": 20, "t": 50, "b": 80},
        legend={"font": {"size": 20}, "orientation": "h", "x": 0.5, "y": -0.15, "xanchor": "center"},
    )
    return apply_plotly_theme(fig)


def make_donut_combustivel_valor(df_filtered: pd.DataFrame) -> go.Figure:
    """Donut: valor em R$ por tipo de combustível."""
    if df_filtered.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados por combustível")
        return apply_plotly_theme(fig)

    by_fuel = df_filtered.groupby("combustivel", as_index=False).agg(valor=("valor", "sum"))
    fig = go.Figure(go.Pie(
        labels=by_fuel["combustivel"], values=by_fuel["valor"], hole=0.5,
        marker_colors=[FUEL_COLORS.get(c, "#2563eb") for c in by_fuel["combustivel"]],
        textinfo="label+percent",
        hovertemplate="<b>%{label}</b><br>R$ %{value:,.2f}<br>%{percent}<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_dark", title="Combustível por valor (R$)",
        margin={"l": 20, "r": 20, "t": 50, "b": 80},
        legend={"font": {"size": 14}, "orientation": "h", "x": 0.5, "y": -0.15, "xanchor": "center"},
    )
    return apply_plotly_theme(fig)


# ---------------------------------------------------------------------------
# Consumo mensal por tipo (barras agrupadas)
# ---------------------------------------------------------------------------

def _make_bar_consumo_tipo_mes_base(
    df_filtered: pd.DataFrame,
    value_col: str,
    yaxis_title: str,
    title: str,
    text_fmt,
) -> go.Figure:
    """Base comum para gráficos de consumo por tipo e mês (R$ ou Litros)."""
    if df_filtered.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title=f"Sem dados — {title}")
        return apply_plotly_theme(fig)

    from core.services.normalization import classify_fuel_group
    df = df_filtered.copy()
    df["combustivel_grupo"] = df["combustivel"].map(classify_fuel_group)
    df = df[df["combustivel_grupo"].isin(["GASOLINA", "DIESEL S10", "ALCOOL"])]
    if df.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title=f"Sem dados — {title}")
        return apply_plotly_theme(fig)

    grupo = (
        df.groupby(["ano", "mes", "mes_nome", "combustivel_grupo"], as_index=False)
        .agg(total=(value_col, "sum"))
        .sort_values(["ano", "mes", "combustivel_grupo"])
    )
    grupo["periodo"] = grupo.apply(
        lambda r: f"{_corrige_mes(r['mes_nome'])}/{int(r['ano'])}", axis=1
    )

    fig = go.Figure()
    color_map = {"GASOLINA": FUEL_COLORS["GASOLINA"], "DIESEL S10": FUEL_COLORS["DIESEL S10"], "ALCOOL": FUEL_COLORS["ALCOOL"]}
    for fuel in ["GASOLINA", "DIESEL S10", "ALCOOL"]:
        dados = grupo[grupo["combustivel_grupo"] == fuel]
        if dados.empty:
            continue
        fig.add_trace(go.Bar(
            x=dados["periodo"], y=dados["total"],
            name=fuel.title(), marker_color=color_map[fuel],
            text=[text_fmt(v) for v in dados["total"]],
            textposition="outside",
            offsetgroup=fuel, legendgroup=fuel, showlegend=True,
            textfont={"size": 13, "color": "#fff", "family": "'Space Grotesk', sans-serif"},
        ))

    fig.update_layout(
        template="plotly_dark", title=title,
        xaxis_title="Período", yaxis_title=yaxis_title,
        barmode="group", bargap=0.18, bargroupgap=0.08,
        margin={"l": 20, "r": 20, "t": 48, "b": 60},
        legend={"orientation": "h", "x": 0.5, "y": -0.18, "xanchor": "center", "yanchor": "top",
                "font": {"size": 13, "color": "#eaf2ff"}},
    )
    return apply_plotly_theme(fig)


def make_bar_consumo_tipo_mes(df_filtered: pd.DataFrame) -> go.Figure:
    """Consumo em R$ por tipo e mês (barras agrupadas)."""
    return _make_bar_consumo_tipo_mes_base(
        df_filtered, "valor", "Valor faturado (R$)",
        "Consumo de combustível por mês e tipo (R$)",
        lambda v: currency(v),
    )


def make_bar_consumo_tipo_mes_litros(df_filtered: pd.DataFrame) -> go.Figure:
    """Consumo em litros por tipo e mês (barras agrupadas)."""
    return _make_bar_consumo_tipo_mes_base(
        df_filtered, "litros", "Volume (Litros)",
        "Consumo de combustível por mês e tipo (Litros)",
        lambda v: f"{v:,.0f} L",
    )


# ---------------------------------------------------------------------------
# Consumo por secretaria (horizontal stacked)
# ---------------------------------------------------------------------------

def make_bar_consumo_secretaria(
    status_df: pd.DataFrame,
    df_limits: pd.DataFrame | None = None,
) -> go.Figure:
    """
    Barras horizontais empilhadas: consumo até limite + excesso + saldo empenho.

    Ordenado pelo percentual consumido (mais crítico no topo).
    """
    if status_df is None or status_df.empty or "secretaria" not in status_df.columns:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados por secretaria")
        return apply_plotly_theme(fig)

    consumo = status_df.copy()
    consumo["secretaria"] = consumo["secretaria"].str.strip().str.upper()
    consumo = consumo[consumo["gasto_valor"] > 0].copy()

    if consumo.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados de consumo")
        return apply_plotly_theme(fig)

    consumo["pct"] = consumo.apply(
        lambda r: r["gasto_valor"] / r["limite_valor_periodo"] * 100
        if r.get("limite_valor_periodo", 0) > 0 else 0,
        axis=1,
    )
    consumo = consumo.sort_values("pct", ascending=True)
    consumo["consumo_ate_limite"] = consumo.apply(
        lambda r: min(r["gasto_valor"], r.get("limite_valor_periodo", r["gasto_valor"]))
        if r.get("limite_valor_periodo", 0) > 0 else r["gasto_valor"],
        axis=1,
    )
    consumo["excesso"] = (
        (consumo["gasto_valor"] - consumo.get("limite_valor_periodo", consumo["gasto_valor"]))
        .clip(lower=0)
        if "limite_valor_periodo" in consumo.columns
        else pd.Series(0, index=consumo.index)
    )
    if "valor_empenhado_total" in consumo.columns:
        consumo["saldo_empenho"] = (
            consumo["valor_empenhado_total"] - consumo["consumo_ate_limite"] - consumo["excesso"]
        ).clip(lower=0)
    else:
        consumo["saldo_empenho"] = 0

    def _label(v: float, pct: float) -> str:
        return f"{pct:.0f}%  ·  {currency(v)}" if v > 0 else ""

    bar_colors = [_bar_color_pct(p) for p in consumo["pct"]]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=consumo["secretaria"], x=consumo["consumo_ate_limite"], orientation="h",
        name="Consumo até limite", marker_color=bar_colors,
        text=[_label(v, p) for v, p in zip(consumo["consumo_ate_limite"], consumo["pct"])],
        textposition="inside", insidetextanchor="start",
        textfont={"color": "#fff", "size": 12},
        hovertemplate="<b>%{y}</b><br>Consumo: R$ %{x:,.2f}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        y=consumo["secretaria"], x=consumo["excesso"], orientation="h",
        name="Excesso sobre limite", marker_color="#dc2626",
        text=[currency(v) if v > 0 else "" for v in consumo["excesso"]],
        textposition="inside", textfont={"color": "#fff", "size": 11},
        hovertemplate="<b>%{y}</b><br>Excesso: R$ %{x:,.2f}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        y=consumo["secretaria"], x=consumo["saldo_empenho"], orientation="h",
        name="Saldo do empenho", marker_color="#475569", marker_opacity=0.5,
        text=[currency(v) if v > 0 else "" for v in consumo["saldo_empenho"]],
        textposition="inside", insidetextanchor="end",
        textfont={"color": "#cbd5e1", "size": 11},
        hovertemplate="<b>%{y}</b><br>Saldo: R$ %{x:,.2f}<extra></extra>",
    ))
    fig.update_layout(
        barmode="stack", template="plotly_dark",
        title="Consumo por secretaria vs. limite (R$)",
        xaxis_title="R$",
        margin={"l": 20, "r": 20, "t": 48, "b": 60},
        height=max(480, 42 * len(consumo)),
        bargap=0.25,
        legend={"orientation": "h", "x": 0.5, "y": -0.08, "xanchor": "center", "yanchor": "top",
                "font": {"size": 13, "color": "#eaf2ff"}},
    )
    return apply_plotly_theme(fig)


# ---------------------------------------------------------------------------
# % do limite consumido por secretaria (valor e litros)
# ---------------------------------------------------------------------------

def make_bar_valor_vs_limite_secretaria(status_df: pd.DataFrame) -> go.Figure:
    """Barras horizontais: % do limite de R$ consumido por secretaria."""
    if status_df.empty or "gasto_valor" not in status_df.columns:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados")
        return apply_plotly_theme(fig)

    df = status_df[status_df["limite_valor_periodo"] > 0].copy()
    if df.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem limites definidos")
        return apply_plotly_theme(fig)

    df["pct"] = df["gasto_valor"] / df["limite_valor_periodo"] * 100
    df["desvio_valor"] = df["gasto_valor"] - df["limite_valor_periodo"]
    df = df.sort_values("pct", ascending=True)

    _ref = df[df["limite_mensal"] > 0]
    months_label = "período"
    if not _ref.empty:
        _m = round((_ref["limite_valor_periodo"] / _ref["limite_mensal"]).mean())
        months_label = f"1 mês" if _m <= 1 else f"{int(_m)} meses"

    colors = [_bar_color_pct(p) for p in df["pct"]]
    pct_clip = df["pct"].clip(upper=200)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df["secretaria"], x=pct_clip, orientation="h",
        marker_color=colors,
        text=[f"⚠ {p:.0f}%" if p > 100 else f"{p:.0f}%" for p in df["pct"]],
        textposition="outside", textfont={"size": 12, "color": "#eaf2ff"},
        customdata=list(zip(df["gasto_valor"], df["limite_valor_periodo"], df["pct"], df["desvio_valor"])),
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Gasto: R$ %{customdata[0]:,.2f}<br>"
            "Limite (" + months_label + "): R$ %{customdata[1]:,.2f}<br>"
            "Uso: %{customdata[2]:.1f}%<br>"
            "Saldo/Excesso: R$ %{customdata[3]:,.2f}<extra></extra>"
        ),
        showlegend=False,
    ))
    fig.add_vline(x=100, line_dash="dash", line_color="#eab308", line_width=2)
    max_pct = max(df["pct"].max(), 100)
    fig.update_layout(
        template="plotly_dark", title="Gasto em R$ vs. limite por secretaria",
        xaxis_title="% do limite consumido",
        xaxis={"range": [0, max_pct * 1.18], "ticksuffix": "%"},
        margin={"l": 20, "r": 20, "t": 48, "b": 30},
        height=max(400, 30 * len(df)),
    )
    return apply_plotly_theme(fig)


def make_bar_litros_vs_limite_secretaria(
    df_filtered: pd.DataFrame, df_limits: pd.DataFrame
) -> go.Figure:
    """Barras horizontais: % do limite de litros por secretaria e combustível."""
    if df_filtered.empty or df_limits.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados")
        return apply_plotly_theme(fig)

    from core.services.normalization import classify_fuel_group
    months = month_count(df_filtered)
    df = df_filtered.copy()
    df["combustivel_grupo"] = df["combustivel"].map(classify_fuel_group)
    df = df[df["combustivel_grupo"].isin(["GASOLINA", "DIESEL S10", "ALCOOL"])]
    consumed = df.groupby(["secretaria", "combustivel_grupo"], as_index=False).agg(litros=("litros", "sum"))

    fuel_limit_col = {"GASOLINA": "limite_litros_gasolina", "ALCOOL": "limite_litros_alcool", "DIESEL S10": "limite_litros_diesel"}
    rows = []
    for _, row in df_limits.iterrows():
        for fuel, col in fuel_limit_col.items():
            lim = float(row.get(col, 0)) * months
            if lim > 0:
                rows.append({"secretaria": row["secretaria"], "combustivel_grupo": fuel, "limite": lim})
    if not rows:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem limites de litros definidos")
        return apply_plotly_theme(fig)

    limits_df = pd.DataFrame(rows)
    merged = limits_df.merge(consumed, on=["secretaria", "combustivel_grupo"], how="left").fillna(0)
    merged["pct"] = merged.apply(lambda r: r["litros"] / r["limite"] * 100 if r["limite"] > 0 else 0, axis=1)
    merged["label_y"] = merged["secretaria"] + " · " + merged["combustivel_grupo"].str.title()
    merged = merged.sort_values(["secretaria", "combustivel_grupo"])

    colors = [_bar_color_pct(p) for p in merged["pct"]]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=merged["label_y"], x=merged["pct"].clip(upper=150), orientation="h",
        marker_color=colors,
        text=[f"{p:.0f}% ({v:,.0f} L / {l:,.0f} L)" for p, v, l in zip(merged["pct"], merged["litros"], merged["limite"])],
        textposition="outside", textfont={"size": 12, "color": "#eaf2ff"},
        customdata=list(zip(merged["litros"], merged["limite"], merged["pct"])),
        hovertemplate="<b>%{y}</b><br>Consumido: %{customdata[0]:,.0f} L<br>Limite: %{customdata[1]:,.0f} L<br>Uso: %{customdata[2]:.1f}%<extra></extra>",
        showlegend=False,
    ))
    fig.add_vline(x=100, line_dash="dash", line_color="#eab308", line_width=2)
    fig.update_layout(
        template="plotly_dark", title="Consumo de litros vs. limite por secretaria",
        xaxis_title="% do limite consumido",
        xaxis={"range": [0, 155], "ticksuffix": "%"},
        margin={"l": 20, "r": 60, "t": 48, "b": 30},
        height=max(400, 28 * len(merged)),
    )
    return apply_plotly_theme(fig)


# ---------------------------------------------------------------------------
# Ranking de secretaria
# ---------------------------------------------------------------------------

def make_ranking_consumo_secretaria(status_df: pd.DataFrame) -> go.Figure:
    """Ranking horizontal: gasto + saldo do empenho por secretaria."""
    if status_df.empty or "gasto_valor" not in status_df.columns:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados")
        return apply_plotly_theme(fig)

    df = status_df[status_df["gasto_valor"] > 0].copy()
    df["saldo_empenho"] = (
        (df["valor_empenhado_total"] - df["gasto_valor"]).clip(lower=0)
        if "valor_empenhado_total" in df.columns
        else 0
    )
    df = df.sort_values("gasto_valor", ascending=True)
    max_val = (df["valor_empenhado_total"] if "valor_empenhado_total" in df.columns else df["gasto_valor"]).max()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df["secretaria"], x=df["gasto_valor"], orientation="h",
        name="Gasto", marker_color="#2563eb",
        text=[currency(v) for v in df["gasto_valor"]],
        textposition="inside", insidetextanchor="start",
        textfont={"size": 12, "color": "#fff"},
        hovertemplate="<b>%{y}</b><br>Gasto: R$ %{x:,.2f}<extra></extra>",
    ))
    if "valor_empenhado_total" in df.columns:
        fig.add_trace(go.Bar(
            y=df["secretaria"], x=df["saldo_empenho"], orientation="h",
            name="Saldo do empenho", marker_color="#475569", marker_opacity=0.5,
            text=[currency(v) if v > 0 else "" for v in df["saldo_empenho"]],
            textposition="inside", insidetextanchor="end",
            textfont={"size": 11, "color": "#cbd5e1"},
            hovertemplate="<b>%{y}</b><br>Saldo: R$ %{x:,.2f}<extra></extra>",
        ))
    fig.update_layout(
        barmode="stack", template="plotly_dark",
        title="Ranking de Consumo por Secretaria (R$)", xaxis_title="R$",
        xaxis={"range": [0, max_val * 1.02]},
        margin={"l": 20, "r": 20, "t": 48, "b": 60},
        height=max(480, 42 * len(df)), bargap=0.25,
        legend={"orientation": "h", "x": 0.5, "y": -0.08, "xanchor": "center"},
    )
    return apply_plotly_theme(fig)


# ---------------------------------------------------------------------------
# Top N veículos
# ---------------------------------------------------------------------------

def make_ranking_veiculos(df_filtered: pd.DataFrame, top_n: int = 20) -> go.Figure:
    """Top N veículos por gasto total, identificados por Marca Modelo — PLACA."""
    needed = {"placa", "marca", "modelo", "valor", "litros"}
    if df_filtered.empty or not needed.issubset(df_filtered.columns):
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Ranking de veículos — sem dados")
        return apply_plotly_theme(fig)

    has_sec = "secretaria" in df_filtered.columns
    group_cols = ["marca", "modelo", "placa"] + (["secretaria"] if has_sec else [])
    grp = (
        df_filtered.groupby(group_cols, as_index=False)
        .agg(valor=("valor", "sum"), litros=("litros", "sum"), abastecimentos=("placa", "count"))
        .sort_values("valor", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    grp["rank"] = range(1, len(grp) + 1)
    grp = grp.sort_values("valor", ascending=True).reset_index(drop=True)

    def _label(row) -> str:
        v = f"{row['marca'].strip().title()} {row['modelo'].strip().title()} — {row['placa'].strip().upper()}"
        if has_sec and row.get("secretaria", ""):
            v += f"  |  {str(row['secretaria']).strip()}"
        return f"#{int(row['rank'])}  {v}"

    labels_y = [_label(row) for _, row in grp.iterrows()]
    max_val = grp["valor"].max() if not grp.empty else 1

    fig = go.Figure(go.Bar(
        y=labels_y, x=grp["valor"], orientation="h",
        marker={"color": "#2563eb", "line": {"width": 0}},
        text=[f"R$ {v:,.0f}".replace(",", ".") for v in grp["valor"]],
        textposition="outside",
        textfont={"size": 12, "color": "#eaf2ff", "family": "Rajdhani, sans-serif"},
        customdata=list(zip(grp["litros"], grp["abastecimentos"])),
        hovertemplate=(
            "<b>%{y}</b><br>Gasto: R$ %{x:,.2f}<br>"
            "Litros: %{customdata[0]:,.1f} L<br>"
            "Abastecimentos: %{customdata[1]}<extra></extra>"
        ),
        showlegend=False,
    ))
    fig.update_layout(
        template="plotly_dark",
        title=f"🏆 Top {top_n} veículos por gasto",
        xaxis={"title": "Gasto total (R$)", "tickprefix": "R$ ", "range": [0, max_val * 1.22]},
        yaxis={"tickfont": {"size": 12, "family": "Rajdhani, sans-serif"}},
        margin={"l": 10, "r": 20, "t": 50, "b": 30},
        height=max(480, 32 * top_n), bargap=0.35,
    )
    return apply_plotly_theme(fig)


# ---------------------------------------------------------------------------
# Treemap de postos
# ---------------------------------------------------------------------------

def make_treemap_postos(df_filtered: pd.DataFrame) -> go.Figure:
    """Treemap dos postos de abastecimento por valor total gasto."""
    if df_filtered.empty or "posto" not in df_filtered.columns:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Treemap de postos — sem dados")
        return apply_plotly_theme(fig)

    df = df_filtered.copy()
    df["posto"] = df["posto"].fillna("Desconhecido").str.strip()
    df = df[df["posto"] != ""]
    grp = df.groupby("posto", as_index=False).agg(
        valor=("valor", "sum"), litros=("litros", "sum"), abastecimentos=("placa", "count")
    ).sort_values("valor", ascending=False)

    fig = go.Figure(go.Treemap(
        labels=grp["posto"], parents=[""] * len(grp), values=grp["valor"],
        customdata=list(zip(grp["litros"], grp["abastecimentos"])),
        texttemplate="<b>%{label}</b><br>R$ %{value:,.0f}",
        hovertemplate="<b>%{label}</b><br>Gasto: R$ %{value:,.2f}<br>Litros: %{customdata[0]:,.1f} L<br>Abastecimentos: %{customdata[1]}<extra></extra>",
        marker={
            "colorscale": [[0.0, "#1e3a5f"], [0.5, "#1d6fa4"], [1.0, "#38bdf8"]],
            "colors": grp["valor"].tolist(), "showscale": False,
        },
        textfont={"size": 13, "color": "#eaf2ff"},
    ))
    fig.update_layout(
        template="plotly_dark", title="Postos de abastecimento — gasto total (R$)",
        margin={"l": 10, "r": 10, "t": 48, "b": 10}, height=520,
    )
    return apply_plotly_theme(fig)


# ---------------------------------------------------------------------------
# Linhas de custo médio
# ---------------------------------------------------------------------------

def make_line_custo_medio_mes_combustivel(df_filtered: pd.DataFrame) -> go.Figure:
    """Volume em litros por tipo de combustível ao longo dos meses."""
    if df_filtered.empty or not {"mes", "ano", "combustivel", "litros"}.issubset(df_filtered.columns):
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Consumo por combustível por mês")
        return apply_plotly_theme(fig)

    grupo = df_filtered.groupby(["ano", "mes", "combustivel"], as_index=False).agg(litros=("litros", "sum"))
    grupo["mes_label"] = grupo["mes"].map(MONTHS)

    fig = go.Figure()
    for comb in grupo["combustivel"].unique():
        dados = grupo[grupo["combustivel"] == comb]
        cor = FUEL_COLORS.get(str(comb).upper(), "#38bdf8")
        fig.add_trace(go.Scatter(
            x=dados["mes_label"], y=dados["litros"],
            mode="lines+markers", name=str(comb),
            text=[f"{v:,.0f} L" for v in dados["litros"]],
            textposition="top center",
            line={"color": cor, "width": 3}, marker={"color": cor},
        ))
    fig.update_layout(
        template="plotly_dark", title="Consumo por combustível por mês (Litros)",
        xaxis_title="Mês", yaxis_title="Volume (Litros)",
        margin={"l": 30, "r": 30, "t": 48, "b": 30},
        legend={"font": {"size": 13, "color": "#eaf2ff"}},
    )
    return apply_plotly_theme(fig)


def make_line_custo_medio_rl_combustivel(df_filtered: pd.DataFrame) -> go.Figure:
    """Custo médio R$/L por combustível ao longo dos meses."""
    if df_filtered.empty or not {"mes", "combustivel", "valor", "litros"}.issubset(df_filtered.columns):
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Custo médio R$/L por combustível")
        return apply_plotly_theme(fig)

    df = df_filtered.copy()
    df["combustivel"] = df["combustivel"].replace({"ETANOL": "ALCOOL"})
    grupo = df.groupby(["mes", "combustivel"], as_index=False).agg(valor=("valor", "sum"), litros=("litros", "sum"))
    grupo = grupo[grupo["litros"] > 0].copy()
    grupo["custo_medio"] = grupo["valor"] / grupo["litros"]
    grupo["mes_label"] = grupo["mes"].map(MONTHS)
    grupo = grupo.sort_values("mes")

    fig = go.Figure()
    for comb in sorted(grupo["combustivel"].unique()):
        dados = grupo[grupo["combustivel"] == comb].sort_values("mes")
        cor = FUEL_COLORS.get(str(comb).upper(), "#94a3b8")
        fig.add_trace(go.Scatter(
            x=dados["mes_label"], y=dados["custo_medio"],
            mode="lines+markers", name=str(comb),
            line={"color": cor, "width": 3}, marker={"color": cor, "size": 7},
            hovertemplate=f"<b>%{{x}}</b><br>{comb}<br>Custo médio: R$ %{{y:.3f}}/L<extra></extra>",
        ))
    fig.update_layout(
        template="plotly_dark", title="Custo médio R$/L por combustível",
        xaxis={"title": "Mês", "categoryorder": "array", "categoryarray": [MONTHS[m] for m in sorted(MONTHS)]},
        yaxis_title="R$/L", yaxis_tickprefix="R$ ", yaxis_tickformat=".2f",
        margin={"l": 30, "r": 20, "t": 48, "b": 60},
        legend={"orientation": "h", "x": 0.5, "y": -0.18, "xanchor": "center",
                "font": {"size": 13, "color": "#eaf2ff"}},
    )
    return apply_plotly_theme(fig)


# ---------------------------------------------------------------------------
# Acumulado real vs. previsto
# ---------------------------------------------------------------------------

def make_line_real_previsto_projecao(
    df_filtered: pd.DataFrame,
    df_limits: pd.DataFrame,
) -> go.Figure:
    """Gasto acumulado real vs. previsto (linha anual)."""
    if df_filtered.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Série acumulada — sem dados")
        return apply_plotly_theme(fig)

    mensal_real = (
        df_filtered.groupby(["ano", "mes"], as_index=False)
        .agg(valor=("valor", "sum"))
        .sort_values(["ano", "mes"])
    )
    mensal_real["acumulado_real"] = mensal_real["valor"].cumsum()
    mensal_real["mes_label"] = mensal_real["mes"].map(MONTHS)

    empenho_total = float(df_limits["valor_empenhado"].sum()) if not df_limits.empty else 0.0
    previsto_mensal = empenho_total / 12.0
    meses_previstos = [MONTHS[m] for m in range(1, 13)]
    acumulado_previsto = [previsto_mensal * (i + 1) for i in range(12)]
    previsto_map = dict(zip(meses_previstos, acumulado_previsto))

    xs = mensal_real["mes_label"].tolist()
    ys = mensal_real["acumulado_real"].tolist()
    acima = [y > previsto_map.get(x, float("inf")) for x, y in zip(xs, ys)]
    marker_colors = ["#ef4444" if a else "#22c55e" for a in acima]

    fig = go.Figure()
    # Segmentos coloridos (verde = dentro do previsto, vermelho = acima)
    i = 0
    legend_green = legend_red = False
    while i < len(xs):
        cor = "#ef4444" if acima[i] else "#22c55e"
        nome = "Acima do previsto" if acima[i] else "Real acumulado"
        show = (acima[i] and not legend_red) or (not acima[i] and not legend_green)
        seg_x, seg_y = [xs[i]], [ys[i]]
        while i + 1 < len(xs) and acima[i + 1] == acima[i]:
            i += 1
            seg_x.append(xs[i])
            seg_y.append(ys[i])
        if i + 1 < len(xs):
            seg_x.append(xs[i + 1])
            seg_y.append(ys[i + 1])
        fig.add_trace(go.Scatter(x=seg_x, y=seg_y, mode="lines", name=nome,
                                  line={"color": cor, "width": 3}, showlegend=show,
                                  legendgroup=nome, hoverinfo="skip"))
        if acima[i]: legend_red = True
        else: legend_green = True
        i += 1

    fig.add_trace(go.Scatter(
        x=xs, y=ys, mode="markers+text", showlegend=False,
        marker={"color": marker_colors, "size": 10, "line": {"color": "#fff", "width": 1}},
        text=[f"{v:,.0f}".replace(",", ".") for v in ys],
        textposition="bottom center", textfont={"size": 14},
        hovertemplate="<b>%{x}</b><br>Real acumulado: R$ %{y:,.2f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=meses_previstos, y=acumulado_previsto, mode="lines+markers",
        name="Previsto acumulado", line={"color": "#f4a259", "width": 3, "dash": "dash"},
    ))
    fig.update_layout(
        template="plotly_dark", title="Gasto acumulado: Real x Previsto",
        xaxis_title="Mês", yaxis_title="Valor acumulado",
        margin={"l": 30, "r": 30, "t": 48, "b": 60},
        legend={"orientation": "h", "x": 0.5, "y": -0.18, "xanchor": "center",
                "font": {"size": 13, "color": "#eaf2ff"}},
    )
    return apply_plotly_theme(fig)


# ---------------------------------------------------------------------------
# Comparativos YoY
# ---------------------------------------------------------------------------

def _make_yoy_bars(
    df_scope: pd.DataFrame,
    value_col: str,
    data_inicio,
    data_fim,
    title: str,
    yaxis_title: str,
    text_fmt,
    hovertemplate_suffix: str,
) -> go.Figure:
    """Base comum para gráficos de barras comparativas YoY."""
    required = {"ano", "mes", value_col}
    if df_scope.empty or not required.issubset(df_scope.columns):
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title=title)
        return apply_plotly_theme(fig)

    meses_filtro = list(range(1, 13))
    if data_inicio and data_fim and data_inicio.year == data_fim.year:
        meses_filtro = list(range(data_inicio.month, data_fim.month + 1))

    df = df_scope[df_scope["mes"].isin(meses_filtro)].copy()
    grupo = df.groupby(["ano", "mes"], as_index=False).agg(total=(value_col, "sum"))
    if grupo.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title=title)
        return apply_plotly_theme(fig)

    anos = sorted(grupo["ano"].unique())
    meses_presentes = sorted(grupo["mes"].unique())
    meses_labels = [MONTHS_SHORT[m] for m in meses_presentes]

    fig = go.Figure()
    for ano in anos:
        dados = grupo[grupo["ano"] == ano].set_index("mes")
        y_vals = [float(dados.loc[m, "total"]) if m in dados.index else None for m in meses_presentes]
        fig.add_trace(go.Bar(
            name=str(int(ano)), x=meses_labels, y=y_vals,
            marker_color=YEAR_PALETTE.get(int(ano), "#94a3b8"),
            text=[text_fmt(v) if v else "" for v in y_vals],
            textposition="outside", textfont={"size": 11, "color": "#e7eef8"},
            hovertemplate=f"<b>%{{x}} {int(ano)}</b><br>{hovertemplate_suffix}<extra></extra>",
        ))

    fig.update_layout(
        template="plotly_dark", title=title, barmode="group",
        bargap=0.18, bargroupgap=0.05,
        xaxis_title="Mês", yaxis_title=yaxis_title,
        margin={"l": 30, "r": 20, "t": 54, "b": 60},
        legend={"orientation": "h", "x": 0.02, "y": 0.99, "xanchor": "left", "yanchor": "top",
                "font": {"size": 13, "color": "#eaf2ff"}, "bgcolor": "rgba(8,17,28,0.65)"},
    )
    return apply_plotly_theme(fig)


def make_bar_comparativo_mensal_yoy(df_scope: pd.DataFrame, data_inicio, data_fim) -> go.Figure:
    """Barras agrupadas: mesmo mês em diferentes anos (R$)."""
    title = "Comparativo Mensal por Ano"
    if data_inicio and data_fim and data_inicio.year == data_fim.year:
        title = f"Comparativo Mensal: {data_inicio.year} vs anos anteriores"
    return _make_yoy_bars(df_scope, "valor", data_inicio, data_fim, title,
                          "Valor (R$)", currency, "R$ %{y:,.2f}")


def make_bar_comparativo_mensal_yoy_litros(df_scope: pd.DataFrame, data_inicio, data_fim) -> go.Figure:
    """Barras agrupadas: mesmo mês em diferentes anos (Litros)."""
    return _make_yoy_bars(
        df_scope, "litros", data_inicio, data_fim,
        "Comparativo Mensal por Ano (Litros)", "Litros",
        lambda v: f"{v:,.0f} L".replace(",", "."),
        "%{y:,.0f} L",
    )


def make_line_sazonalidade_yoy(df_scope: pd.DataFrame) -> go.Figure:
    """Overlay mensal por ano para comparação de sazonalidade."""
    required = {"ano", "mes", "valor"}
    if df_scope.empty or not required.issubset(df_scope.columns):
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Comparativo de Sazonalidade YoY")
        return apply_plotly_theme(fig)

    grupo = df_scope.groupby(["ano", "mes"], as_index=False).agg(valor_total=("valor", "sum")).sort_values(["ano", "mes"])
    if grupo.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Comparativo de Sazonalidade YoY")
        return apply_plotly_theme(fig)

    month_order = [MONTHS_SHORT[m] for m in range(1, 13)]
    anos = sorted(int(a) for a in grupo["ano"].dropna().unique())
    ano_atual = max(anos)
    styles_antigos = [
        {"color": "#bfdbfe", "dash": "dot", "width": 2},
        {"color": "#38bdf8", "dash": "dash", "width": 3},
        {"color": "#60a5fa", "dash": "dashdot", "width": 2},
    ]

    fig = go.Figure()
    for idx, ano in enumerate([a for a in anos if a != ano_atual]):
        dados = grupo[grupo["ano"] == ano].copy()
        dados["mes_label"] = dados["mes"].map(MONTHS_SHORT)
        s = styles_antigos[min(idx, len(styles_antigos) - 1)]
        fig.add_trace(go.Scatter(
            x=dados["mes_label"], y=dados["valor_total"], mode="lines", name=str(ano),
            line={"color": s["color"], "width": s["width"], "dash": s["dash"]},
            hovertemplate=f"<b>%{{x}}</b><br>Ano: {ano}<br>Valor: R$ %{{y:,.2f}}<extra></extra>",
        ))

    dados_atual = grupo[grupo["ano"] == ano_atual].copy()
    dados_atual["mes_label"] = dados_atual["mes"].map(MONTHS_SHORT)
    fig.add_trace(go.Scatter(
        x=dados_atual["mes_label"], y=dados_atual["valor_total"],
        mode="lines+markers", name=f"{ano_atual} (Atual)",
        line={"color": "#1d4ed8", "width": 4}, marker={"color": "#1d4ed8", "size": 8},
        hovertemplate=f"<b>%{{x}}</b><br>Ano: {ano_atual} (Atual)<br>Valor: R$ %{{y:,.2f}}<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_dark", title="Comparativo de Sazonalidade YoY",
        xaxis={"title": "Mês", "categoryorder": "array", "categoryarray": month_order},
        yaxis_title="Valor total (R$)", yaxis_tickprefix="R$ ", yaxis_tickformat=",.0f",
        margin={"l": 30, "r": 20, "t": 54, "b": 60},
        legend={"orientation": "h", "x": 0.02, "y": 0.99, "xanchor": "left", "yanchor": "top",
                "font": {"size": 13, "color": "#eaf2ff"}, "bgcolor": "rgba(8,17,28,0.65)"},
    )
    return apply_plotly_theme(fig)


# ---------------------------------------------------------------------------
# Gráfico legado mantido para compatibilidade (usado na aba Resumo)
# ---------------------------------------------------------------------------

def make_bar_consumo_combustivel(df_filtered: pd.DataFrame) -> go.Figure:
    """Barras verticais: valor total por tipo de combustível."""
    if df_filtered.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados por combustível")
        return apply_plotly_theme(fig)

    by_fuel = df_filtered.groupby("combustivel", as_index=False).agg(valor=("valor", "sum"))
    fig = go.Figure(go.Bar(
        x=by_fuel["combustivel"],
        y=by_fuel["valor"],
        marker_color=[FUEL_COLORS.get(c, "#2563eb") for c in by_fuel["combustivel"]],
        text=[currency(v) for v in by_fuel["valor"]],
        textposition="outside",
        textfont={"size": 20, "color": "#fff", "family": "'Space Grotesk', sans-serif"},
        name="Consumo de combustível",
    ))
    fig.update_layout(
        template="plotly_dark", title="Consumo de combustível",
        xaxis_title="Tipo de combustível", yaxis_title="Valor faturado (R$)",
        margin={"l": 20, "r": 20, "t": 50, "b": 20},
    )
    return apply_plotly_theme(fig)
