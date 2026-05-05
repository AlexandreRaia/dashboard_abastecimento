"""
Cards de KPI para o dashboard de abastecimento.

Renderiza os dois painéis de KPI via HTML injetado no Streamlit,
com suporte a badges de variação YoY (Year-over-Year).
"""
from __future__ import annotations

import streamlit as st

from core.utils.formatters import currency


def _delta_badge(
    key: str,
    deltas: dict | None,
    *,
    inverted: bool = False,
    neutral: bool = False,
) -> str:
    """
    Gera o HTML do badge de variação YoY para um KPI.

    Args:
        key:      Chave do KPI no dicionário deltas.
        deltas:   Dicionário {chave: (pct_variação, ano_referência)}.
        inverted: Se True, alta é ruim (ex.: gasto de litros).
        neutral:  Se True, badge cinza sem conotação de bom/ruim.

    Returns:
        String HTML do badge, ou string vazia se não houver dado.
    """
    if not deltas or key not in deltas:
        return ""
    pct, ano_ref = deltas[key]
    if pct is None or abs(pct) < 0.5:  # suprime variações insignificantes
        return ""

    is_up = pct > 0
    arrow = "▲" if is_up else "▼"

    if neutral:
        color, bg = "#94a3b8", "rgba(148,163,184,0.15)"
    else:
        is_bad = is_up if inverted else not is_up
        color = "#f87171" if is_bad else "#4ade80"
        bg = "rgba(239,68,68,0.15)" if is_bad else "rgba(34,197,94,0.15)"

    return (
        f'<div class="kpi-delta" style="color:{color};background:{bg};">'
        f'{arrow} {abs(pct):.1f}% vs {ano_ref}</div>'
    )


def render_kpi_cards(
    kpis: dict,
    deltas: dict | None = None,
) -> None:
    """
    Renderiza os painéis de KPI financeiros e operacionais.

    Args:
        kpis:   Dicionário retornado por build_kpis().
        deltas: Dicionário opcional de variações YoY por chave de KPI.
                Formato: {chave: (percentual_float, ano_referência_str)}.
    """

    def _fmt(v: float, dec: int = 0) -> str:
        """Formata número com separador de milhar brasileiro."""
        return f"{v:,.{dec}f}".replace(",", "X").replace(".", ",").replace("X", ".")

    # Painel 1: KPIs financeiros
    html = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="kpi-label">{kpis['label_valor_empenhado']}</div>
            <div class="kpi-value">{currency(kpis['valor_empenhado'])}</div>
            {_delta_badge('valor_empenhado', deltas, neutral=True)}
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Gasto Total Faturado</div>
            <div class="kpi-value">{currency(kpis['gasto_total'])}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">{kpis['label_saldo_empenho']}</div>
            <div class="kpi-value">{currency(kpis['saldo_empenho'])}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Média mensal de consumo</div>
            <div class="kpi-value">{currency(kpis['media_mensal_consumo'])}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Saldo cobre quantos meses</div>
            <div class="kpi-value">{kpis['cobertura']:.1f} {'⚠️' if kpis['cobertura'] < 3 else 'meses'}</div>
            {'<div class="kpi-delta" style="color:#f87171;background:rgba(239,68,68,0.15);">Atenção: saldo baixo</div>' if kpis['cobertura'] < 3 else ''}
        </div>
    </div>
    <div class="kpi-grid" style="margin-top:8px;">
        <div class="kpi-card">
            <div class="kpi-label">Total de litros consumidos</div>
            <div class="kpi-value">{_fmt(kpis['gasto_litros'], 2)} L</div>
            {_delta_badge('gasto_litros', deltas, inverted=True)}
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Eficiência média da frota</div>
            <div class="kpi-value">{_fmt(kpis['consumo_medio'], 2)} km/L</div>
            {_delta_badge('consumo_medio', deltas, inverted=False)}
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Custo médio por km rodado</div>
            <div class="kpi-value">{currency(kpis['custo_por_km'])}</div>
            {_delta_badge('custo_por_km', deltas, neutral=True)}
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Total de abastecimentos</div>
            <div class="kpi-value">{_fmt(kpis['n_abastecimentos'])}</div>
            {_delta_badge('n_abastecimentos', deltas, neutral=True)}
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Veículos ativos no período</div>
            <div class="kpi-value">{kpis['veiculos_ativos']}</div>
            {_delta_badge('veiculos_ativos', deltas, inverted=False)}
        </div>
    </div>
    """
    # Remove indentação de tabs — Streamlit interpreta linhas com tab como código
    html_clean = "\n".join(line.lstrip() for line in html.splitlines())
    st.markdown(html_clean, unsafe_allow_html=True)
