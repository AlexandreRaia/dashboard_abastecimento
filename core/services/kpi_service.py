"""
Cálculo de KPIs e métricas de secretaria.

Esta é a camada de regras de negócio para o dashboard financeiro.
Todas as funções são puras (sem I/O) e recebem DataFrames já filtrados.
"""
from __future__ import annotations

import datetime
import pandas as pd

from config.constants import MONTHS
from core.services.normalization import classify_fuel_group


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _corrige_mes_nome(periodo: str) -> str:
    """Corrige 'marco' → 'Março' em strings de período Mês/Ano."""
    partes = periodo.split("/")
    if len(partes) == 2 and partes[0].strip().lower() == "marco":
        return "Março/" + partes[1]
    return periodo


def month_count(df: pd.DataFrame) -> int:
    """Retorna o número de meses distintos presentes no DataFrame."""
    if df.empty or "ano_mes" not in df.columns:
        return 1
    return max(1, int(df["ano_mes"].nunique()))


# ---------------------------------------------------------------------------
# Mistura mensal de combustíveis
# ---------------------------------------------------------------------------

def build_monthly_mix(
    df_filtered: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Agrega consumo mensal por tipo de combustível.

    Args:
        df_filtered: DataFrame filtrado com colunas 'combustivel', 'valor',
                     'litros', 'ano', 'mes', 'mes_nome'.

    Returns:
        Tupla (value_mix, monthly_totals):
        - value_mix:      Agregação por (ano, mes, combustivel_grupo) com
                          participação percentual e rótulo de período.
        - monthly_totals: Totais mensais com variação percentual MoM.
    """
    chart_df = df_filtered.copy()
    chart_df["combustivel_grupo"] = chart_df["combustivel"].map(classify_fuel_group)
    chart_df = chart_df[chart_df["combustivel_grupo"].isin(["GASOLINA", "DIESEL S10", "ALCOOL"])]

    if chart_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    value_mix = (
        chart_df.groupby(
            ["ano", "mes", "mes_nome", "combustivel_grupo"], as_index=False
        )
        .agg(valor_total=("valor", "sum"), litros_total=("litros", "sum"))
        .sort_values(["ano", "mes", "combustivel_grupo"])
    )
    value_mix["periodo"] = value_mix.apply(
        lambda row: f"{row['mes_nome']}/{int(row['ano'])}", axis=1
    )

    monthly_totals = (
        value_mix.groupby(["ano", "mes", "mes_nome", "periodo"], as_index=False)
        .agg(
            valor_total_mes=("valor_total", "sum"),
            litros_total_mes=("litros_total", "sum"),
        )
        .sort_values(["ano", "mes"])
    )
    monthly_totals["variacao_pct"] = (
        monthly_totals["valor_total_mes"].pct_change().fillna(0.0) * 100.0
    )
    monthly_totals["media_valor"] = monthly_totals["valor_total_mes"].mean()

    # Período corrigido (Março sem cedilha → com cedilha)
    monthly_totals["periodo_corrigido"] = monthly_totals["periodo"].map(_corrige_mes_nome)

    value_mix = value_mix.merge(
        monthly_totals[["periodo", "valor_total_mes", "litros_total_mes", "variacao_pct"]],
        on="periodo",
        how="left",
    )
    value_mix["participacao_pct"] = value_mix.apply(
        lambda row: (row["valor_total"] / row["valor_total_mes"] * 100.0)
        if row["valor_total_mes"] > 0
        else 0.0,
        axis=1,
    )
    dominant_by_month = value_mix.groupby("periodo")["valor_total"].transform("max")
    value_mix["is_dominante"] = value_mix["valor_total"] == dominant_by_month
    value_mix["texto_pct"] = value_mix.apply(
        lambda row: f"{row['participacao_pct']:.0f}%"
        if row["is_dominante"] and row["participacao_pct"] >= 8
        else "",
        axis=1,
    )

    return value_mix, monthly_totals


# ---------------------------------------------------------------------------
# Status por secretaria
# ---------------------------------------------------------------------------

def build_secretaria_status(
    df_filtered: pd.DataFrame, df_limits: pd.DataFrame
) -> pd.DataFrame:
    """
    Calcula status financeiro de cada secretaria versus o limite do período.

    Leva em conta múltiplos anos no período (pro-rata de meses por ano).

    Args:
        df_filtered: DataFrame de abastecimentos filtrado pelo período.
        df_limits:   DataFrame de parâmetros financeiros (pode ter múltiplas
                     linhas por secretaria, uma por ano).

    Returns:
        DataFrame com colunas: secretaria, gasto_valor, gasto_litros,
        limite_valor_periodo, limite_litros_periodo, desvio_valor,
        desvio_pct, estourou_valor, estourou_litros, status.
    """
    if df_filtered.empty or df_limits.empty:
        return pd.DataFrame()

    # Gasto real por secretaria
    real = df_filtered.groupby("secretaria", as_index=False).agg(
        gasto_valor=("valor", "sum"), gasto_litros=("litros", "sum")
    )

    # Meses por ano no período filtrado
    months_by_year: dict[int, int] = {}
    if "data_hora" in df_filtered.columns and not df_filtered.empty:
        aux = df_filtered.copy()
        aux["_ano"] = pd.to_datetime(aux["data_hora"]).dt.year
        aux["_ano_mes"] = pd.to_datetime(aux["data_hora"]).dt.to_period("M")
        months_by_year = aux.groupby("_ano")["_ano_mes"].nunique().to_dict()

    base = df_limits.copy()
    for col in ("limite_mensal", "limite_litros_mensal"):
        if col not in base.columns:
            base[col] = 0.0
    if "ano" not in base.columns:
        base["ano"] = datetime.date.today().year

    # Agregar limites pro-rata por secretaria
    aggregated = []
    for sec in base["secretaria"].unique():
        sec_rows = base[base["secretaria"] == sec]
        lim_valor = lim_litros = emp_total = lim_mensal_sum = lim_lit_mensal_sum = 0.0
        for _, row in sec_rows.iterrows():
            ano = int(row["ano"])
            meses = int(months_by_year.get(ano, 0))
            if meses > 0:
                lim_valor += float(row["limite_mensal"]) * meses
                lim_litros += float(row["limite_litros_mensal"]) * meses
            emp_total += float(row.get("valor_empenhado", 0.0))
            lim_mensal_sum += float(row["limite_mensal"])
            lim_lit_mensal_sum += float(row["limite_litros_mensal"])

        n = len(sec_rows)
        aggregated.append(
            {
                "secretaria": sec,
                "limite_valor_periodo": lim_valor,
                "limite_litros_periodo": lim_litros,
                "valor_empenhado_total": emp_total,
                "limite_mensal": lim_mensal_sum / n if n > 0 else 0.0,
                "limite_litros_mensal": lim_lit_mensal_sum / n if n > 0 else 0.0,
            }
        )

    base_agg = pd.DataFrame(aggregated)
    merged = base_agg.merge(real, on="secretaria", how="left").fillna(0.0)

    merged["desvio_valor"] = merged["gasto_valor"] - merged["limite_valor_periodo"]
    merged["desvio_pct"] = merged.apply(
        lambda r: (r["desvio_valor"] / r["limite_valor_periodo"] * 100.0)
        if r["limite_valor_periodo"] > 0
        else 0.0,
        axis=1,
    )
    merged["estourou_valor"] = merged["gasto_valor"] > merged["limite_valor_periodo"]
    merged["estourou_litros"] = merged["gasto_litros"] > merged["limite_litros_periodo"]
    merged["estouro_preco"] = merged["estourou_valor"] & ~merged["estourou_litros"]
    merged["status"] = merged.apply(
        lambda r: "ESTOURO POR PRECO"
        if r["estouro_preco"]
        else ("ESTOURO GERAL" if (r["estourou_valor"] and r["estourou_litros"]) else "OK"),
        axis=1,
    )
    return merged


# ---------------------------------------------------------------------------
# KPIs consolidados
# ---------------------------------------------------------------------------

def build_kpis(
    df_filtered: pd.DataFrame,
    status_df: pd.DataFrame,
    df_limits: pd.DataFrame,
) -> dict:
    """
    Calcula os KPIs financeiros e operacionais do período filtrado.

    Args:
        df_filtered: Registros de abastecimento do período.
        status_df:   Resultado de build_secretaria_status().
        df_limits:   Parâmetros financeiros do período.

    Returns:
        Dicionário com todos os KPIs prontos para render_kpi_cards().
    """
    gasto_total = float(df_filtered["valor"].sum())
    gasto_bruto_total = float(
        df_filtered["valor_bruto"].sum()
        if "valor_bruto" in df_filtered.columns
        else gasto_total
    )
    desconto_total = float(
        df_filtered["desconto_valor"].sum()
        if "desconto_valor" in df_filtered.columns
        else 0.0
    )
    gasto_litros = float(df_filtered["litros"].sum())
    km_total = float(
        df_filtered["km_rodado"].sum() if "km_rodado" in df_filtered.columns else 0.0
    )
    consumo_medio = (
        float(df_filtered["km_por_litro"].mean())
        if "km_por_litro" in df_filtered.columns
        and df_filtered["km_por_litro"].notna().any()
        else 0.0
    )
    custo_por_km = (
        float(df_filtered["custo_por_km"].mean())
        if "custo_por_km" in df_filtered.columns
        and df_filtered["custo_por_km"].notna().any()
        else 0.0
    )
    n_abastecimentos = len(df_filtered)
    veiculos_ativos = (
        int(df_filtered["placa"].nunique()) if "placa" in df_filtered.columns else 0
    )

    valor_empenhado = float(df_limits["valor_empenhado"].sum()) if not df_limits.empty else 0.0
    saldo_empenho = valor_empenhado - gasto_total
    months = month_count(df_filtered)
    gasto_medio_mensal = gasto_total / months if months else 0.0
    cobertura = saldo_empenho / gasto_medio_mensal if gasto_medio_mensal > 0 else 0.0

    return {
        "valor_empenhado":      valor_empenhado,
        "label_valor_empenhado": "Valor Empenhado",
        "gasto_total":          gasto_total,
        "gasto_bruto_total":    gasto_bruto_total,
        "desconto_total":       desconto_total,
        "gasto_litros":         gasto_litros,
        "saldo_empenho":        saldo_empenho,
        "label_saldo_empenho":  "Saldo Total",
        "media_mensal_consumo": gasto_medio_mensal,
        "cobertura":            cobertura,
        # Operacionais
        "km_total":             km_total,
        "consumo_medio":        consumo_medio,
        "custo_por_km":         custo_por_km,
        "n_abastecimentos":     n_abastecimentos,
        "veiculos_ativos":      veiculos_ativos,
    }


# ---------------------------------------------------------------------------
# Alertas e ranking
# ---------------------------------------------------------------------------

def build_alerts(status_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra secretarias com status diferente de OK para exibição de alertas.

    Returns:
        DataFrame com secretarias em alerta, ordenado por desvio (maior primeiro).
    """
    alerts = status_df[status_df["status"] != "OK"].copy()
    if alerts.empty:
        return pd.DataFrame(
            [{"secretaria": "Sem alertas", "status": "OK", "desvio_pct": 0.0, "desvio_valor": 0.0}]
        )
    return alerts[["secretaria", "status", "desvio_pct", "desvio_valor"]].sort_values(
        "desvio_pct", ascending=False
    )


def build_ranking(status_df: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna ranking de secretarias ordenado por desvio percentual.

    Returns:
        DataFrame com colunas de gasto, limite, desvio e status.
    """
    cols = [
        "secretaria",
        "gasto_valor",
        "limite_valor_periodo",
        "desvio_pct",
        "gasto_litros",
        "limite_litros_periodo",
        "status",
    ]
    available = [c for c in cols if c in status_df.columns]
    return status_df[available].copy().sort_values("desvio_pct", ascending=False)
