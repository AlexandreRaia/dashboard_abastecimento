from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "relatorio.db"
CONFIG_PATH = BASE_DIR / "config.json"
DEFAULT_DISCOUNT_RATE = 0.0405

MONTHS = {
	1: "Janeiro",
	2: "Fevereiro",
	3: "Marco",
	4: "Abril",
	5: "Maio",
	6: "Junho",
	7: "Julho",
	8: "Agosto",
	9: "Setembro",
	10: "Outubro",
	11: "Novembro",
	12: "Dezembro",
}
MONTH_NAME_TO_NUMBER = {v: k for k, v in MONTHS.items()}

FUEL_MAP = {
	"GASOLINA": "GASOLINA",
	"ALCOOL": "ALCOOL",
	"ETANOL": "ALCOOL",
	"DIESEL": "DIESEL",
}

THEME = {
	"bg": "#0a121b",
	"panel": "#111d2b",
	"panel_soft": "#162436",
	"text": "#e7eef8",
	"muted": "#8ea3be",
	"accent": "#2dd4bf",
	"accent2": "#38bdf8",
	"warning": "#f59e0b",
	"danger": "#ef4444",
	"ok": "#22c55e",
}


def normalize_secretaria(value: str) -> str:
	return str(value or "").strip().upper()


def normalize_fuel(value: str) -> str:
	raw = str(value or "").strip().upper()
	return FUEL_MAP.get(raw, raw)


def clamp_discount_rate(value: float) -> float:
	return min(max(float(value), 0.0), 1.0)


def load_config(path: Path) -> tuple[pd.DataFrame, float]:
	with path.open("r", encoding="utf-8") as file:
		payload = json.load(file)

	discount_rate = clamp_discount_rate(payload.get("desconto_percentual", DEFAULT_DISCOUNT_RATE))

	rows = []
	for item in payload.get("secretarias", []):
		litros = item.get("limites_litros", {})
		row = {
			"secretaria": normalize_secretaria(item.get("sigla")),
			"empenho_2026": float(item.get("empenho_2026", 0.0)),
			"limite_quinzenal": float(item.get("limite_quinzenal", 0.0)),
			"limite_litros_gasolina": float(litros.get("gasolina", 0.0)),
			"limite_litros_alcool": float(litros.get("alcool", 0.0)),
			"limite_litros_diesel": float(litros.get("diesel", 0.0)),
		}
		row["limite_litros_mensal"] = (
			row["limite_litros_gasolina"]
			+ row["limite_litros_alcool"]
			+ row["limite_litros_diesel"]
		)
		rows.append(row)

	return pd.DataFrame(rows), discount_rate


def resolve_source_table(conn: sqlite3.Connection) -> str:
	tables = pd.read_sql_query(
		"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
		conn,
	)["name"].tolist()

	if "abastecimentos" in tables:
		return "abastecimentos"
	if "plan1" in tables:
		return "plan1"
	raise RuntimeError("Nenhuma tabela de abastecimento encontrada (abastecimentos/plan1).")


def load_sqlite(path: Path) -> pd.DataFrame:
	with sqlite3.connect(path) as conn:
		table_name = resolve_source_table(conn)
		query = f"""
			SELECT
				"Data/Hora" AS data_hora,
				"Unidade" AS secretaria,
				"Produto" AS combustivel,
				"Vr. Unit." AS valor_unitario,
				"Qtde (L)" AS litros,
				"Valor" AS valor
			FROM {table_name}
		"""
		df = pd.read_sql_query(query, conn)

	df["data_hora"] = pd.to_datetime(df["data_hora"], errors="coerce")
	df["secretaria"] = df["secretaria"].map(normalize_secretaria)
	df["combustivel"] = df["combustivel"].map(normalize_fuel)
	df["valor_unitario"] = pd.to_numeric(df["valor_unitario"], errors="coerce").fillna(0.0)
	df["litros"] = pd.to_numeric(df["litros"], errors="coerce").fillna(0.0)
	df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)

	df = df.dropna(subset=["data_hora"])
	df["ano"] = df["data_hora"].dt.year
	df["mes"] = df["data_hora"].dt.month
	df["mes_nome"] = df["mes"].map(MONTHS)
	df["ano_mes"] = df["data_hora"].dt.to_period("M").astype(str)
	return df


def apply_discount(df: pd.DataFrame, discount_rate: float) -> pd.DataFrame:
	out = df.copy()
	out["valor_bruto"] = out["valor"]
	if "valor_unitario" not in out.columns:
		out["valor_unitario"] = out.apply(
			lambda row: (row["valor"] / row["litros"]) if row.get("litros", 0) else 0.0,
			axis=1,
		)
	out["valor_unitario_bruto"] = pd.to_numeric(out["valor_unitario"], errors="coerce").fillna(0.0)
	out["valor"] = out["valor_bruto"] * (1.0 - discount_rate)
	out["valor_unitario"] = out["valor_unitario_bruto"] * (1.0 - discount_rate)
	out["desconto_valor"] = out["valor_bruto"] - out["valor"]
	return out


def apply_filters(
	df: pd.DataFrame,
	ano: str,
	mes: str,
	secretaria: str,
	combustivel: str,
) -> pd.DataFrame:
	out = df.copy()

	if ano != "Todos":
		out = out[out["ano"] == int(ano)]

	if mes != "Todos":
		month_number = MONTH_NAME_TO_NUMBER.get(mes)
		if month_number is not None:
			out = out[out["mes"] == month_number]

	if secretaria != "Todas":
		out = out[out["secretaria"] == normalize_secretaria(secretaria)]

	if combustivel != "Todos":
		out = out[out["combustivel"] == normalize_fuel(combustivel)]

	return out


def month_count(df: pd.DataFrame) -> int:
	if df.empty:
		return 1
	return max(1, int(df["ano_mes"].nunique()))


def build_secretaria_status(df_filtered: pd.DataFrame, df_limits: pd.DataFrame) -> pd.DataFrame:
	months = month_count(df_filtered)
	quinzenas = months * 2

	real = (
		df_filtered.groupby("secretaria", as_index=False)
		.agg(gasto_valor=("valor", "sum"), gasto_litros=("litros", "sum"))
	)

	base = df_limits.copy()
	base["limite_valor_periodo"] = base["limite_quinzenal"] * quinzenas
	base["limite_litros_periodo"] = base["limite_litros_mensal"] * months

	merged = base.merge(real, on="secretaria", how="left").fillna(0.0)
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
		else ("ESTOURO GERAL" if r["estourou_valor"] and r["estourou_litros"] else "OK"),
		axis=1,
	)
	return merged


def build_kpis(
	df_filtered: pd.DataFrame,
	status_df: pd.DataFrame,
	df_limits: pd.DataFrame,
	usar_limite_quinzenal_secretaria: bool = False,
) -> dict[str, float | str]:
	gasto_total = float(df_filtered["valor"].sum())
	gasto_bruto_total = float(df_filtered.get("valor_bruto", df_filtered["valor"]).sum())
	desconto_total = float(df_filtered.get("desconto_valor", 0.0).sum())
	gasto_litros = float(df_filtered["litros"].sum())

	limite_total_periodo = float(status_df["limite_valor_periodo"].sum())

	months = month_count(df_filtered)
	valor_empenhado = float(df_limits["empenho_2026"].sum())
	saldo_empenho = valor_empenhado - gasto_total
	gasto_medio_mensal = gasto_total / months if months else 0.0
	cobertura = saldo_empenho / gasto_medio_mensal if gasto_medio_mensal > 0 else 0.0

	return {
		"valor_empenhado": valor_empenhado,
		"label_valor_empenhado": "Valor Empenhado",
		"gasto_total": gasto_total,
		"gasto_bruto_total": gasto_bruto_total,
		"desconto_total": desconto_total,
		"gasto_litros": gasto_litros,
		"limite_total": limite_total_periodo,
		"saldo_empenho": saldo_empenho,
		"label_saldo_empenho": "Saldo Total",
		"cobertura": cobertura,
	}


def make_line_real_vs_previsto(
	df_filtered: pd.DataFrame,
	df_limits: pd.DataFrame,
	usar_limite_quinzenal_secretaria: bool = False,
) -> go.Figure:
	if df_filtered.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Serie acumulada sem dados")
		return apply_plotly_theme(fig)
		return fig

	mensal_real = (
		df_filtered.groupby(["ano", "mes", "mes_nome"], as_index=False)
		.agg(valor=("valor", "sum"))
		.sort_values(["ano", "mes"])
	)
	mensal_real["acumulado_real"] = mensal_real["valor"].cumsum()

	if usar_limite_quinzenal_secretaria and not df_limits.empty:
		previsto_mensal = float(df_limits["limite_quinzenal"].sum()) * 2.0
	else:
		empenho_total = float(df_limits["empenho_2026"].sum())
		previsto_mensal = empenho_total / 12.0
	mensal_real["acumulado_previsto"] = [previsto_mensal * (i + 1) for i in range(len(mensal_real))]

	fig = go.Figure()
	fig.add_trace(
		go.Scatter(
			x=mensal_real["mes_nome"],
			y=mensal_real["acumulado_real"],
			mode="lines+markers",
			name="Real acumulado",
			line={"color": "#23b5d3", "width": 3},
		)
	)
	fig.add_trace(
		go.Scatter(
			x=mensal_real["mes_nome"],
			y=mensal_real["acumulado_previsto"],
			mode="lines+markers",
			name="Previsto acumulado",
			line={"color": "#f4a259", "width": 3, "dash": "dash"},
		)
	)
	fig.update_layout(
		template="plotly_dark",
		title={"text": "Gasto faturado acumulado: Real x Previsto", "x": 0.01, "y": 0.98},
		margin={"l": 30, "r": 30, "t": 78, "b": 30},
		legend={
			"orientation": "h",
			"x": 0.01,
			"y": 1.03,
			"yanchor": "bottom",
			"font": {"size": 28, "color": "#eaf2ff"},
			"bgcolor": "rgba(8, 17, 28, 0.75)",
		},
	)
	return apply_plotly_theme(fig)


def build_monthly_mix(df_filtered: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
	chart_df = df_filtered.copy()
	chart_df["combustivel_grupo"] = chart_df["combustivel"].map(
		lambda value: "DIESEL"
		if str(value).upper().startswith("DIESEL")
		else ("ALCOOL" if str(value).upper() in {"ALCOOL", "ETANOL"} else str(value).upper())
	)
	chart_df = chart_df[chart_df["combustivel_grupo"].isin(["GASOLINA", "DIESEL", "ALCOOL"])]

	if chart_df.empty:
		return pd.DataFrame(), pd.DataFrame()

	value_mix = (
		chart_df.groupby(["ano", "mes", "mes_nome", "combustivel_grupo"], as_index=False)
		.agg(valor_total=("valor", "sum"), litros_total=("litros", "sum"))
		.sort_values(["ano", "mes", "combustivel_grupo"])
	)
	value_mix["periodo"] = value_mix.apply(lambda row: f"{row['mes_nome']}/{int(row['ano'])}", axis=1)

	monthly_totals = (
		value_mix.groupby(["ano", "mes", "mes_nome", "periodo"], as_index=False)
		.agg(valor_total_mes=("valor_total", "sum"), litros_total_mes=("litros_total", "sum"))
		.sort_values(["ano", "mes"])
	)
	monthly_totals["variacao_pct"] = monthly_totals["valor_total_mes"].pct_change().fillna(0.0) * 100.0
	monthly_totals["media_valor"] = monthly_totals["valor_total_mes"].mean()
	monthly_totals["media_litros"] = monthly_totals["litros_total_mes"].mean()

	value_mix = value_mix.merge(
		monthly_totals[["periodo", "valor_total_mes", "litros_total_mes", "variacao_pct"]],
		on="periodo",
		how="left",
	)
	value_mix["participacao_pct"] = value_mix.apply(
		lambda row: (row["valor_total"] / row["valor_total_mes"] * 100.0) if row["valor_total_mes"] > 0 else 0.0,
		axis=1,
	)
	dominant_by_month = value_mix.groupby("periodo")["valor_total"].transform("max")
	value_mix["is_dominante"] = value_mix["valor_total"] == dominant_by_month
	value_mix["texto_pct"] = value_mix.apply(
		lambda row: f"{row['participacao_pct']:.0f}%" if row["is_dominante"] and row["participacao_pct"] >= 8 else "",
		axis=1,
	)
	return value_mix, monthly_totals


def make_bar_gasto_por_mes(df_filtered: pd.DataFrame) -> go.Figure:
	value_mix, monthly_totals = build_monthly_mix(df_filtered)
	fig = go.Figure()
	if value_mix.empty or monthly_totals.empty:
		fig.update_layout(template="plotly_dark", title="Consumo por mês sem dados")
		return apply_plotly_theme(fig)

	fig.add_trace(
		go.Bar(
			x=monthly_totals["periodo"],
			y=monthly_totals["valor_total_mes"],
			name="Consumo do mês",
			marker={"color": "#2563eb", "line": {"color": "#102a56", "width": 1.5}},
			text=[currency(value) for value in monthly_totals["valor_total_mes"]],
			textposition="outside",
			textfont={"size": 18, "color": "#fff", "family": "'Space Grotesk', sans-serif"},
			insidetextanchor="end",
			insidetextfont={"color": "#fff", "size": 18, "family": "'Space Grotesk', sans-serif"},
			outsidetextfont={"color": "#fff", "size": 18, "family": "'Space Grotesk', sans-serif"},
			texttemplate="<span style='background-color:#222;padding:4px 8px;border-radius:6px'><b>%{text}</b></span>",
			customdata=list(
				zip(
					monthly_totals["variacao_pct"],
					monthly_totals["litros_total_mes"],
					monthly_totals["media_valor"],
				)
			),
			hovertemplate=(
				"Período: %{x}<br>"
				+ "Consumo: R$ %{y:,.2f}<br>"
				+ "Variação vs mês anterior: %{customdata[0]:+.1f}%<br>"
				+ "Litros: %{customdata[1]:,.0f}<br>"
				+ "Média mensal: R$ %{customdata[2]:,.2f}<extra></extra>"
			),
		)
	)
	fig.add_trace(
		go.Scatter(
			x=monthly_totals["periodo"],
			y=monthly_totals["media_valor"],
			mode="lines",
			name="Média mensal",
			line={"color": "#f59e0b", "width": 3, "dash": "dash"},
			hovertemplate="Período: %{x}<br>Média mensal: R$ %{y:,.2f}<extra></extra>",
		)
	)
	fig.update_layout(
		template="plotly_dark",
		title="Consumo por mês",
		xaxis_title="Período",
		yaxis_title="Valor faturado",
		margin={"l": 30, "r": 30, "t": 72, "b": 30},
		bargap=0.28,
		legend={"orientation": "h", "x": 0.01, "y": 1.02, "xanchor": "left", "yanchor": "bottom"},
	)
	return apply_plotly_theme(fig)


def make_scatter_preco_tempo(df_filtered: pd.DataFrame) -> go.Figure:
	if df_filtered.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Evolucao do valor unitario sem dados")
		return apply_plotly_theme(fig)

	scatter_df = df_filtered[df_filtered["valor_unitario"] > 0].copy()
	if scatter_df.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Evolucao do valor unitario sem dados")
		return apply_plotly_theme(fig)

	scatter_df["combustivel_grupo"] = scatter_df["combustivel"].map(
		lambda value: "DIESEL"
		if str(value).upper().startswith("DIESEL")
		else ("ALCOOL" if str(value).upper() in {"ALCOOL", "ETANOL"} else str(value).upper())
	)
	scatter_df = scatter_df[scatter_df["combustivel_grupo"].isin(["GASOLINA", "DIESEL", "ALCOOL"])]
	if scatter_df.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Evolucao do valor unitario sem dados")
		return apply_plotly_theme(fig)

	scatter_df["data_label"] = scatter_df["data_hora"].dt.strftime("%d/%m/%Y")
	scatter_df["periodo"] = scatter_df["data_hora"].dt.strftime("%m/%Y")

	fig = px.scatter(
		scatter_df,
		x="data_hora",
		y="valor_unitario",
		color="combustivel_grupo",
		color_discrete_map={"GASOLINA": "#2563eb", "DIESEL": "#fb7185", "ALCOOL": "#f97316"},
		custom_data=["secretaria", "data_label", "periodo", "litros", "valor"],
		hover_data={
			"secretaria": False,
			"data_label": False,
			"periodo": False,
			"valor_unitario": ":.3f",
			"litros": False,
			"valor": False,
		},
		labels={
			"data_hora": "Data",
			"valor_unitario": "Valor unitario",
			"valor": "Valor faturado",
			"combustivel_grupo": "Combustível",
			"data_label": "Data",
		},
	)
	fig.update_traces(
		marker={"size": 9, "opacity": 0.62, "line": {"width": 1, "color": "rgba(255,255,255,0.18)"}},
		hovertemplate=(
			"Data: %{customdata[1]}<br>"
			"Secretaria: %{customdata[0]}<br>"
			"Periodo: %{customdata[2]}<br>"
			"Valor unitario: R$ %{y:,.3f}<br>"
			"Litros: %{customdata[3]:,.3f}<br>"
			"Valor do registro: R$ %{customdata[4]:,.2f}<extra>%{fullData.name}</extra>"
		),
	)
	fig.update_layout(
		template="plotly_dark",
		title="Valor unitario ao longo do tempo",
		xaxis_title="Data",
		yaxis_title="Valor unitario (R$/L)",
		margin={"l": 30, "r": 30, "t": 72, "b": 30},
		legend={"orientation": "h", "x": 0.01, "y": 1.02, "xanchor": "left", "yanchor": "bottom"},
	)
	return apply_plotly_theme(fig)


def make_line_preco_medio_mes(df_filtered: pd.DataFrame) -> go.Figure:
	if df_filtered.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Preco medio mensal sem dados")
		return apply_plotly_theme(fig)

	price_df = df_filtered[df_filtered["valor_unitario"] > 0].copy()
	if price_df.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Preco medio mensal sem dados")
		return apply_plotly_theme(fig)

	price_df["combustivel_grupo"] = price_df["combustivel"].map(
		lambda value: "DIESEL"
		if str(value).upper().startswith("DIESEL")
		else ("ALCOOL" if str(value).upper() in {"ALCOOL", "ETANOL"} else str(value).upper())
	)
	price_df = price_df[price_df["combustivel_grupo"].isin(["GASOLINA", "DIESEL", "ALCOOL"])]
	if price_df.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Preco medio mensal sem dados")
		return apply_plotly_theme(fig)

	mensal_preco = (
		price_df.groupby(["ano", "mes", "mes_nome", "combustivel_grupo"], as_index=False)
		.agg(valor_unitario_medio=("valor_unitario", "mean"))
		.sort_values(["ano", "mes", "combustivel_grupo"])
	)
	mensal_preco["periodo"] = mensal_preco.apply(lambda row: f"{row['mes_nome']}/{int(row['ano'])}", axis=1)

	fig = go.Figure()
	color_map = {"GASOLINA": "#2563eb", "DIESEL": "#fb7185", "ALCOOL": "#f97316"}
	for fuel_name in ["GASOLINA", "DIESEL", "ALCOOL"]:
		fuel_data = mensal_preco[mensal_preco["combustivel_grupo"] == fuel_name]
		if fuel_data.empty:
			continue
		fig.add_trace(
			go.Scatter(
				x=fuel_data["periodo"],
				y=fuel_data["valor_unitario_medio"],
				mode="lines+markers",
				name=fuel_name.title(),
				line={"color": color_map[fuel_name], "width": 3},
				marker={"size": 8},
				hovertemplate="Periodo: %{x}<br>Preco medio: R$ %{y:,.3f}<extra>%{fullData.name}</extra>",
			)
		)

	fig.update_layout(
		template="plotly_dark",
		title="Valor unitario medio por mes",
		xaxis_title="Periodo",
		yaxis_title="Valor unitario (R$/L)",
		margin={"l": 30, "r": 30, "t": 72, "b": 30},
		legend={"orientation": "h", "x": 0.01, "y": 1.02, "xanchor": "left", "yanchor": "bottom"},
	)
	return apply_plotly_theme(fig)


def make_donut_combustivel(df_filtered: pd.DataFrame) -> go.Figure:
	if df_filtered.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Sem dados por combustível")
		return apply_plotly_theme(fig)

	by_fuel = df_filtered.groupby("combustivel", as_index=False).agg(valor=("valor", "sum"))
	fig = px.pie(
		by_fuel,
		names="combustivel",
		values="valor",
		hole=0.65,
		color="combustivel",
		color_discrete_map={"GASOLINA": "#1f77b4", "ALCOOL": "#ff7f0e", "DIESEL": "#d62728"},
	)
	fig.update_traces(
		textinfo="percent",
		textposition="outside",
		textfont={"size": 28, "color": "#f8fbff", "family": "'Space Grotesk', sans-serif"},
		marker={"line": {"color": "#0b1626", "width": 2}},
		hoverlabel={
			"bgcolor": "#0b1626",
			"bordercolor": "#38bdf8",
			"font": {"color": "#f8fbff", "size": 16},
		},
		hovertemplate="<b>%{label}</b><br>Valor faturado: R$ %{value:,.2f}<br>Participacao: %{percent}<extra></extra>",
	)
	fig.update_layout(
		template="plotly_dark",
		title="Gasto faturado por tipo de combustível",
		margin={"l": 20, "r": 20, "t": 50, "b": 20},
		legend={"font": {"size": 24, "color": "#e8f1ff"}},
	)
	return apply_plotly_theme(fig)


def make_bar_desvio(status_df: pd.DataFrame) -> go.Figure:
	top = status_df.sort_values("desvio_pct", ascending=False).head(22)
	colors = ["#d62828" if x > 0 else "#2a9d8f" for x in top["desvio_pct"]]

	fig = go.Figure()
	fig.add_trace(
		go.Bar(
			x=top["desvio_pct"],
			y=top["secretaria"],
			orientation="h",
			marker_color=colors,
			text=[f"{x:.1f}%" for x in top["desvio_pct"]],
			textposition="outside",
			name="Desvio %",
		)
	)
	fig.update_layout(
		template="plotly_dark",
		title="Top secretarias por desvio de valor (%)",
		yaxis={"categoryorder": "total ascending"},
		xaxis={"zeroline": True, "zerolinecolor": "#6b7280"},
		margin={"l": 120, "r": 40, "t": 50, "b": 30},
	)
	return apply_plotly_theme(fig)


def make_bullet_secretarias(status_df: pd.DataFrame) -> go.Figure:
		   data = status_df.sort_values("desvio_pct", ascending=False)
		   fig = go.Figure()

		   # Cálculos
		   empenho = data["empenho_2026"]
		   limite_mensal = data["limite_quinzenal"] * 2
		   gasto = data["gasto_valor"]
		   gasto_ate_limite = gasto.clip(upper=limite_mensal)
		   excesso = (gasto - limite_mensal).clip(lower=0)
		   saldo = (empenho - gasto).clip(lower=0)

		   # Azul: gasto até limite
		   fig.add_trace(
			   go.Bar(
				   y=data["secretaria"],
				   x=gasto_ate_limite,
				   orientation="h",
				   name="Gasto até limite",
				   marker_color="#118ab2",
				   opacity=0.95,
			   )
		   )
		   # Vermelho: excesso
		   fig.add_trace(
			   go.Bar(
				   y=data["secretaria"],
				   x=excesso,
				   orientation="h",
				   name="Excesso sobre limite",
				   marker_color="#e63946",
				   opacity=0.95,
			   )
		   )
		   # Cinza claro: saldo do empenho
		   fig.add_trace(
			   go.Bar(
				   y=data["secretaria"],
				   x=saldo,
				   orientation="h",
				   name="Saldo do empenho",
				   marker_color="#cfd8dc",
				   opacity=0.65,
			   )
		   )

		   # Traço preto vertical para o limite mensal
		   for idx, (sec, lim) in enumerate(zip(data["secretaria"], limite_mensal)):
			   fig.add_shape(
				   type="line",
				   x0=lim,
				   x1=lim,
				   y0=idx - 0.4,
				   y1=idx + 0.4,
				   line={"color": "black", "width": 4},
				   xref="x",
				   yref="y",
				   layer="above"
			   )

		   fig.update_layout(
			   barmode="stack",
			   template="plotly_dark",
			   title={"text": "Grafico de bala: empenhado, gasto, excesso e saldo por secretaria", "x": 0.01, "y": 0.98},
			   margin={"l": 120, "r": 40, "t": 92, "b": 30},
			   legend={"orientation": "h", "x": 0.01, "y": 1.03, "xanchor": "left", "yanchor": "bottom"},
			   shapes=fig.layout.shapes,
		   )
		   return apply_plotly_theme(fig)


def build_alerts(status_df: pd.DataFrame) -> pd.DataFrame:
	alerts = status_df[status_df["status"] != "OK"].copy()
	if alerts.empty:
		return pd.DataFrame(
			[{"secretaria": "Sem alertas", "status": "OK", "desvio_pct": 0.0, "desvio_valor": 0.0}]
		)
	return alerts[["secretaria", "status", "desvio_pct", "desvio_valor"]].sort_values("desvio_pct", ascending=False)


def build_ranking(status_df: pd.DataFrame) -> pd.DataFrame:
	cols = [
		"secretaria",
		"gasto_valor",
		"limite_valor_periodo",
		"desvio_pct",
		"gasto_litros",
		"limite_litros_periodo",
		"status",
	]
	ranking = status_df[cols].copy().sort_values("desvio_pct", ascending=False)
	return ranking


def apply_plotly_theme(fig: go.Figure) -> go.Figure:
	fig.update_layout(
		paper_bgcolor=THEME["panel"],
		plot_bgcolor=THEME["panel"],
		font={"color": THEME["text"], "family": "'Space Grotesk', sans-serif"},
		title={"font": {"size": 18, "color": THEME["text"]}},
		legend={"font": {"size": 12, "color": "#e8f1ff"}, "bgcolor": "rgba(8, 17, 28, 0.65)"},
		hoverlabel={"bgcolor": "#0b1626", "bordercolor": "#38bdf8", "font": {"color": "#f8fbff", "size": 14}},
	)
	fig.update_xaxes(gridcolor="rgba(142,163,190,0.20)")
	fig.update_yaxes(gridcolor="rgba(142,163,190,0.20)")
	return fig


def inject_style() -> None:
	st.markdown(
		"""
		<style>
		@import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;700&family=Space+Grotesk:wght@400;500;700&display=swap');

		#MainMenu,
		footer {
			display: none !important;
		}

		header[data-testid="stHeader"] {
			background: transparent !important;
			border-bottom: 0 !important;
		}

		div[data-testid="stToolbar"] {
			right: 0.75rem;
			top: 0.35rem;
			background: transparent !important;
		}

		button[kind="header"],
		button[data-testid="collapsedControl"] {
			background: #162436 !important;
			border: 1px solid rgba(142,163,190,0.35) !important;
			border-radius: 10px !important;
			color: #e7eef8 !important;
			opacity: 1 !important;
			box-shadow: 0 4px 12px rgba(0, 0, 0, 0.35);
		}

		button[kind="header"] svg,
		button[data-testid="collapsedControl"] svg {
			fill: #e7eef8 !important;
		}

		[data-testid="collapsedControl"],
		[data-testid="stSidebarCollapsedControl"] {
			display: block !important;
			visibility: visible !important;
		}

		div[data-testid="stDecoration"] {
			height: 0 !important;
		}

		.stApp {
			background:
				radial-gradient(circle at 20% -10%, rgba(56,189,248,0.20), transparent 35%),
				radial-gradient(circle at 90% 0%, rgba(45,212,191,0.16), transparent 30%),
				#0a121b;
			color: #e7eef8;
			font-family: 'Space Grotesk', sans-serif;
		}

		section[data-testid="stSidebar"] {
			background: linear-gradient(180deg, #0f1826 0%, #0a121b 100%);
			border-right: 1px solid rgba(142,163,190,0.15);
		}

		section[data-testid="stSidebar"] h1,
		section[data-testid="stSidebar"] h2,
		section[data-testid="stSidebar"] h3,
		section[data-testid="stSidebar"] h4,
		section[data-testid="stSidebar"] p,
		section[data-testid="stSidebar"] label,
		section[data-testid="stSidebar"] .stMarkdown,
		section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] {
			color: #dbe7f5 !important;
			opacity: 1 !important;
		}

		section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {
			color: #c9d8ea !important;
			font-weight: 600 !important;
		}

		.block-container {
			padding-top: 0.35rem;
			padding-bottom: 1.2rem;
			max-width: 1600px;
		}

		.kpi-grid {
			display: grid;
			grid-template-columns: repeat(4, minmax(0, 1fr));
			gap: 12px;
			margin-bottom: 8px;
		}

		.kpi-card {
			background: linear-gradient(150deg, #162436 0%, #111d2b 100%);
			border: 1px solid rgba(142,163,190,0.22);
			border-radius: 14px;
			padding: 14px 16px;
			box-shadow: 0 8px 20px rgba(0, 0, 0, 0.25);
		}

		.kpi-label {
			font-size: 0.78rem;
			color: #8ea3be;
			letter-spacing: 0.04em;
			margin-bottom: 2px;
		}

		.kpi-value {
			font-family: 'Rajdhani', sans-serif;
			font-size: 2.0rem;
			line-height: 1.05;
			font-weight: 700;
			color: #e7eef8;
		}

		.section-title {
			font-size: 1rem;
			font-weight: 700;
			margin: 0.45rem 0 0.25rem 0;
			color: #d9e3f0;
		}

		div[data-testid="stDataFrame"] {
			border: 1px solid rgba(142,163,190,0.20);
			border-radius: 10px;
			overflow: hidden;
		}

		@media (max-width: 1100px) {
			.kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
		}
		</style>
		""",
		unsafe_allow_html=True,
	)


def render_kpi_cards(kpis: dict[str, float | str]) -> None:
	html = f"""
	<div class="kpi-grid">
	  <div class="kpi-card">
	    <div class="kpi-label">{kpis['label_valor_empenhado']}</div>
	    <div class="kpi-value">{currency(kpis['valor_empenhado'])}</div>
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
	    <div class="kpi-label">Meses de Cobertura</div>
	    <div class="kpi-value">{kpis['cobertura']:.1f}</div>
	  </div>
	</div>
	"""
	st.markdown(html, unsafe_allow_html=True)


def currency(value: float) -> str:
	return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


@st.cache_data(show_spinner=False)
def get_limits_df() -> pd.DataFrame:
	return load_config(CONFIG_PATH)[0]


@st.cache_data(show_spinner=False)
def get_discount_rate() -> float:
	return load_config(CONFIG_PATH)[1]


@st.cache_data(show_spinner=False)
def get_real_df(cache_version: str = "v2_valor_unitario") -> pd.DataFrame:
	_ = cache_version
	return load_sqlite(DB_PATH)


def run_dashboard() -> None:
	st.set_page_config(page_title="Painel de Abastecimento", page_icon="⛽", layout="wide", initial_sidebar_state="expanded")
	inject_style()

	df_limits = get_limits_df()
	discount_rate = get_discount_rate()
	df_real = apply_discount(get_real_df("v2_valor_unitario"), discount_rate)

	anos_options = ["Todos"] + [str(x) for x in sorted(df_real["ano"].dropna().unique())]
	mes_options = ["Todos"] + [MONTHS[i] for i in sorted(df_real["mes"].dropna().unique())]
	secretaria_options = ["Todas"] + sorted(df_limits["secretaria"].dropna().unique().tolist())
	combustivel_options = ["Todos"] + sorted(df_real["combustivel"].dropna().unique().tolist())
	default_ano_index = anos_options.index("2026") if "2026" in anos_options else 0

	with st.sidebar:
		st.markdown("### Menu Analítico")
		st.caption("Filtros do período e escopo")
		selected_ano = st.selectbox("Ano", anos_options, index=default_ano_index)
		selected_mes = st.selectbox("Mês", mes_options, index=0)
		selected_secretaria = st.selectbox("Secretaria", secretaria_options, index=0)
		selected_combustivel = st.selectbox("Combustível", combustivel_options, index=0)
		st.divider()
		st.caption(f"Desconto contratual: {discount_rate * 100:.2f}%")
		st.caption(f"Base: {DB_PATH.name}")

	filtered = apply_filters(
		df_real,
		selected_ano,
		selected_mes,
		selected_secretaria,
		selected_combustivel,
	)

	limits_scope = df_limits.copy()
	if selected_secretaria != "Todas":
		limits_scope = limits_scope[limits_scope["secretaria"] == normalize_secretaria(selected_secretaria)]

	status = build_secretaria_status(filtered, limits_scope)
	kpis = build_kpis(
		filtered,
		status,
		limits_scope,
		usar_limite_quinzenal_secretaria=selected_secretaria != "Todas",
	)

	render_kpi_cards(kpis)
	st.caption(
		f"Valores monetarios exibidos com desconto contratual de {discount_rate * 100:.2f}% aplicado sobre o valor bruto do abastecimento."
	)

	monthly_scope = apply_filters(df_real, selected_ano, selected_mes, selected_secretaria, "Todos")
	bar_col, donut_col = st.columns([2, 1])

	bar_col.plotly_chart(make_bar_gasto_por_mes(monthly_scope), use_container_width=True)
	donut_col.plotly_chart(make_donut_combustivel(filtered), use_container_width=True)

	st.plotly_chart(
		make_line_real_vs_previsto(
			filtered,
			limits_scope,
			usar_limite_quinzenal_secretaria=selected_secretaria != "Todas",
		),
		use_container_width=True,
	)


	st.plotly_chart(make_bullet_secretarias(status), use_container_width=True)
	st.plotly_chart(make_bar_desvio(status), use_container_width=True)

	st.markdown("<div class='section-title'>Ranking de Secretarias (desvio)</div>", unsafe_allow_html=True)
	ranking = build_ranking(status).rename(
		columns={
			"secretaria": "Secretaria",
			"gasto_valor": "Gasto Faturado",
			"limite_valor_periodo": "Limite Valor",
			"desvio_pct": "Desvio %",
			"gasto_litros": "Gasto Litros",
			"limite_litros_periodo": "Limite Litros",
			"status": "Status",
		}
	)
	st.dataframe(ranking, use_container_width=True, height=420)

	st.markdown("<div class='section-title'>Alertas Criticos</div>", unsafe_allow_html=True)
	alerts = build_alerts(status).rename(
		columns={
			"secretaria": "Secretaria",
			"status": "Status",
			"desvio_pct": "Desvio %",
			"desvio_valor": "Desvio Valor",
		}
	)
	st.dataframe(alerts, use_container_width=True, height=240)

	count_preco = int(status["estouro_preco"].sum())
	count_geral = int((status["status"] == "ESTOURO GERAL").sum())
	if count_preco > 0:
		st.warning(
			f"{count_preco} secretarias estouraram em valor, mas nao em litros (indicativo de aumento de preco)."
		)
	if count_geral > 0:
		st.error(f"{count_geral} secretarias estouraram em valor e litros.")


if __name__ == "__main__":
	run_dashboard()

