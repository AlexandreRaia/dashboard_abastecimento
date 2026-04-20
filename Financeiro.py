import pandas as pd
import plotly.graph_objects as go
from make_bar_consumo_secretaria import make_bar_consumo_secretaria

def make_bar_consumo_tipo_mes(df_filtered: pd.DataFrame) -> go.Figure:
	if df_filtered.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Sem dados para consumo por tipo e mês")
		return apply_plotly_theme(fig)

	# Agrupa por ano, mês, nome do mês e tipo de combustível
	df = df_filtered.copy()
	df["combustivel_grupo"] = df["combustivel"].map(
		lambda value: "DIESEL"
		if str(value).upper().startswith("DIESEL")
		else ("ALCOOL" if str(value).upper() in {"ALCOOL", "ETANOL"} else str(value).upper())
	)
	df = df[df["combustivel_grupo"].isin(["GASOLINA", "DIESEL", "ALCOOL"])]
	if df.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Sem dados para consumo por tipo e mês")
		return apply_plotly_theme(fig)

	grupo = (
		df.groupby(["ano", "mes", "mes_nome", "combustivel_grupo"], as_index=False)
		.agg(valor_total=("valor", "sum"))
		.sort_values(["ano", "mes", "combustivel_grupo"])
	)
	grupo["periodo"] = grupo.apply(lambda row: f"{row['mes_nome']}/{int(row['ano'])}", axis=1)

	# Calcular média por tipo de combustível
	medias = grupo.groupby("combustivel_grupo")["valor_total"].mean()
	color_map = {"GASOLINA": "#2563eb", "DIESEL": "#fb7185", "ALCOOL": "#f97316"}
	fig = go.Figure()
	for idx, fuel in enumerate(["GASOLINA", "DIESEL", "ALCOOL"]):
		dados = grupo[grupo["combustivel_grupo"] == fuel]
		if dados.empty:
			continue
		media = medias.get(fuel, 0)
		azul = [min(v, media) for v in dados["valor_total"]]
		vermelho = [max(0, v - media) for v in dados["valor_total"]]
		total = [a + r for a, r in zip(azul, vermelho)]
		# Barras até a média (base)
		fig.add_trace(
			go.Bar(
				x=dados["periodo"],
				y=azul,
				name=fuel.title(),
				marker_color=color_map[fuel],
				text=[f"R$ {v:,.2f}" if v > 0 and r == 0 else "" for v, r in zip(dados["valor_total"], vermelho)],
				textposition="outside",
				offsetgroup=fuel,
				legendgroup=fuel,
				showlegend=True,
				customdata=list(zip(azul, vermelho, total)),
				hovertemplate="<b>%{x}</b><br>Consumo até média: R$ %{customdata[0]:,.2f}<br>Excesso: R$ %{customdata[1]:,.2f}<br>Total: R$ %{customdata[2]:,.2f}<extra></extra>",
				textfont_size=12,
				textfont_color="#fff",
				textfont_family="'Space Grotesk', sans-serif",
			)
		)
		# Barras excesso (no topo, vermelho, legenda só na primeira vez)
		show_excesso_legend = idx == 0
		fig.add_trace(
			go.Bar(
				x=dados["periodo"],
				y=vermelho,
				name="Excesso sobre média" if show_excesso_legend else None,
				marker_color="#e63946",
				text=[f"R$ {v:,.2f}" if r > 0 else "" for v, r in zip(dados["valor_total"], vermelho)],
				textposition="outside",
				offsetgroup=fuel,
				legendgroup="excesso",
				base=azul,
				showlegend=show_excesso_legend,
				customdata=list(zip(azul, vermelho, total)),
				hovertemplate="<b>%{x}</b><br>Consumo até média: R$ %{customdata[0]:,.2f}<br>Excesso: R$ %{customdata[1]:,.2f}<br>Total: R$ %{customdata[2]:,.2f}<extra></extra>",
				textfont_size=12,
				textfont_color="#fff",
				textfont_family="'Space Grotesk', sans-serif",
			)
		)
		# Valor total no topo da barra empilhada (acima da barra)
		# Texto exatamente no topo da barra mais alta
		# (Removido: agora o texto está no próprio go.Bar)
	fig.update_layout(
		template="plotly_dark",
		xaxis_title={"text": "Período", "font": {"size": 14}},
		yaxis_title={"text": "Valor faturado (R$)", "font": {"size": 14}},
		barmode="relative",
		margin={"l": 30, "r": 30, "t": 90, "b": 30},
		legend={
			"orientation": "h",
			"x": 0.01,
			"y": 1.04,
			"xanchor": "left",
			"yanchor": "bottom",
			"font": {"size": 12, "color": "#eaf2ff"}
		},
	)
	fig = apply_plotly_theme(fig)
	fig.update_layout(title={"text": "Consumo de combustível por mês e tipo", "x": 0.01, "y": 0.98, "font": {"size": 22}})
	return fig
from bar_consumo_combustivel import make_bar_consumo_combustivel
from plotly_utils import apply_plotly_theme
import json
import sqlite3
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

def make_line_custo_medio_mes_combustivel(df_filtered: pd.DataFrame) -> go.Figure:
	if df_filtered.empty or not {'mes', 'ano', 'combustivel', 'valor', 'litros'}.issubset(df_filtered.columns):
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Sem dados para custo médio por mês/combustível")
		fig.update_layout(template="plotly_dark", title="Custo médio de combustível por mês")
		return apply_plotly_theme(fig)

	# Agrupa por ano, mês e combustível
	df = df_filtered.copy()

	grupo = df.groupby(['ano', 'mes', 'combustivel'], as_index=False).agg(
		valor_total=('valor', 'sum'),
		litros=('litros', 'sum')
	)
	grupo['custo_medio'] = grupo['valor_total'] / grupo['litros']
	grupo['mes_label'] = grupo['mes'].apply(lambda m: MONTHS[m])

	fig = go.Figure()
	combustiveis = grupo['combustivel'].unique()
	for combustivel in combustiveis:
		dados = grupo[grupo['combustivel'] == combustivel]
		fig.add_trace(go.Scatter(
			x=dados['mes_label'],
			y=dados['custo_medio'],
			mode='lines+markers',
			name=str(combustivel),
			text=[f"R$ {v:,.3f}".replace(",", ".") for v in dados['custo_medio']],
			textposition="top center"
		))
	fig.update_layout(
		template="plotly_dark",
		xaxis_title="Mês",
		yaxis_title="Custo médio (R$/L)",
		margin={"l": 30, "r": 30, "t": 60, "b": 30},
		legend={"font": {"size": 16, "color": "#eaf2ff"}},
	)
	fig = apply_plotly_theme(fig)
	fig.update_layout(title={"text": "Custo médio de combustível por mês", "x": 0.01, "y": 0.98, "font": {"size": 22}})
	return fig




def make_line_real_previsto_projecao(
	df_filtered: pd.DataFrame,
	df_limits: pd.DataFrame,
	usar_limite_quinzenal_secretaria: bool = False,
) -> go.Figure:
	import numpy as np
	if df_filtered.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Série acumulada sem dados")
		return apply_plotly_theme(fig)

	# Real acumulado
	mensal_real = (
		df_filtered.groupby(["ano", "mes", "mes_nome"], as_index=False)
		.agg(valor=("valor", "sum"))
		.sort_values(["ano", "mes"])
	)
	mensal_real["acumulado_real"] = mensal_real["valor"].cumsum()

	# Previsto acumulado
	if usar_limite_quinzenal_secretaria and not df_limits.empty:
		previsto_mensal = float(df_limits["limite_quinzenal"].sum()) * 2.0
		previsto_total = previsto_mensal * 12
	else:
		empenho_total = float(df_limits["empenho_2026"].sum())
		previsto_total = empenho_total
		previsto_mensal = previsto_total / 12.0

	# Sempre gerar o previsto acumulado para 12 meses
	meses_previstos = [MONTHS[m] for m in range(1, 13)]
	acumulado_previsto = [previsto_mensal * (i + 1) for i in range(12)]

	fig = go.Figure()
	# Linha real acumulado (apenas meses presentes nos dados)
	fig.add_trace(
		go.Scatter(
			x=mensal_real["mes_nome"],
			y=mensal_real["acumulado_real"],
			mode="lines+markers+text",
			name="Real acumulado",
			line={"color": "#23b5d3", "width": 3},
			text=[f"{v:,.0f}".replace(",", ".") for v in mensal_real["acumulado_real"]],
			textposition="bottom center",
			textfont={"size": 14},
		)
	)
	# Linha previsto acumulado (sempre 12 meses)
	fig.add_trace(
		go.Scatter(
			x=meses_previstos,
			y=acumulado_previsto,
			mode="lines+markers",
			name="Previsto acumulado",
			line={"color": "#f4a259", "width": 3, "dash": "dash"},
		)
	)
	fig.update_layout(
		template="plotly_dark",
		title={"text": "Gasto acumulado: Real x Previsto", "x": 0.01, "y": 0.98},
		margin={"l": 30, "r": 30, "t": 78, "b": 30},
		legend={
			"orientation": "h",
			"x": 0.01,
			"y": 1.03,
			"yanchor": "bottom",
			"font": {"size": 18, "color": "#eaf2ff"},
			"bgcolor": "rgba(8, 17, 28, 0.75)"
		},
		xaxis_title="Mês",
		yaxis_title="Valor acumulado",
	)
	return apply_plotly_theme(fig)

def make_line_projecao_gasto_futuro(df_filtered: pd.DataFrame) -> go.Figure:
	if df_filtered.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Projeção de gasto futuro sem dados")
		return apply_plotly_theme(fig)

	# Agrupa por mês
	mensal = (
		df_filtered.groupby(["ano", "mes", "mes_nome"], as_index=False)
		.agg(valor=("valor", "sum"))
		.sort_values(["ano", "mes"])
	)
	mensal["acumulado_real"] = mensal["valor"].cumsum()

	# Projeção: média mensal * meses restantes
	from datetime import datetime
	hoje = datetime.now()
	ano_atual = hoje.year
	mes_atual = hoje.month
	mensal_ano = mensal[mensal["ano"] == ano_atual]
	if mensal_ano.empty:
		media_mensal = 0.0
		acumulado_hoje = 0.0
		mes_ultimo = mes_atual
	else:
		media_mensal = mensal_ano["valor"].mean()
		acumulado_hoje = mensal_ano["valor"].cumsum().iloc[-1]
		mes_ultimo = mensal_ano["mes"].iloc[-1]

	meses_restantes = 12 - mes_ultimo
	projecao = [acumulado_hoje + media_mensal * (i+1) for i in range(meses_restantes)]
	meses_futuros = list(range(mes_ultimo+1, 13))
	meses_futuros_nome = [MONTHS[m] for m in meses_futuros]

	# Monta eixo x completo
	x_real = mensal_ano["mes_nome"].tolist()
	x_proj = meses_futuros_nome

	fig = go.Figure()
	# Linha real
	fig.add_trace(
		go.Scatter(
			x=x_real,
			y=mensal_ano["acumulado_real"],
			mode="lines+markers",
			name="Real acumulado",
			line={"color": "#23b5d3", "width": 3},
		)
	)
	# Linha projetada
	if meses_restantes > 0:
		fig.add_trace(
			go.Scatter(
				x=x_real + x_proj,
				y=list(mensal_ano["acumulado_real"]) + projecao,
				mode="lines+markers",
				name="Projeção acumulada",
				line={"color": "#f59e0b", "width": 3, "dash": "dash"},
			)
		)
	fig.update_layout(
		template="plotly_dark",
		title={"text": "Projeção de Gasto Futuro (acumulado)", "x": 0.01, "y": 0.98},
		margin={"l": 30, "r": 30, "t": 78, "b": 30},
		legend={
			"orientation": "h",
			"x": 0.01,
			"y": 1.03,
			"yanchor": "bottom",
			"font": {"size": 18, "color": "#eaf2ff"},
			"bgcolor": "rgba(8, 17, 28, 0.75)",
		},
		xaxis_title="Mês",
		yaxis_title="Valor acumulado",
	)
	return apply_plotly_theme(fig)

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
					"Valor" AS valor,
					"Placa" AS placa,
					"Km Rodado" AS km_rodado
				FROM {table_name}
			"""
			df = pd.read_sql_query(query, conn)


		df["data_hora"] = pd.to_datetime(df["data_hora"], errors="coerce")
		df["secretaria"] = df["secretaria"].map(normalize_secretaria)
		df["combustivel"] = df["combustivel"].map(normalize_fuel)

		df["valor_unitario"] = pd.to_numeric(df["valor_unitario"], errors="coerce").fillna(0.0)
		df["litros"] = pd.to_numeric(df["litros"], errors="coerce").fillna(0.0)
		df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)
		if "placa" in df.columns:
			df["placa"] = df["placa"].astype(str).str.strip().str.upper()
		if "km_rodado" in df.columns:
			df["km_rodado"] = pd.to_numeric(df["km_rodado"], errors="coerce").fillna(0.0)

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
		"media_mensal_consumo": gasto_medio_mensal,
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

	# Barras: azul até a média, vermelho para o excesso sobre a média
	azul = []
	vermelho = []
	media = monthly_totals["media_valor"]
	for v, m in zip(monthly_totals["valor_total_mes"], media):
		azul.append(min(v, m))
		vermelho.append(max(0, v - m))

	bar_text = [f"R$ {value:,.2f}" for value in monthly_totals["valor_total_mes"]]

	# Valor total para cada barra (azul+vermelho)
	total_bar = [a + v for a, v in zip(azul, vermelho)]

	fig.add_trace(
		go.Bar(
			x=monthly_totals["periodo"],
			y=azul,
			name="Consumo até média",
			marker={"color": "#2563eb", "line": {"color": "#102a56", "width": 1.5}},
			text=None,
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
		go.Bar(
			x=monthly_totals["periodo"],
			y=vermelho,
			name="Excesso sobre média",
			marker={"color": "#e63946", "line": {"color": "#102a56", "width": 1.5}},
			text=None,
			showlegend=True,
		)
	)
	# Adiciona o valor total no topo da barra empilhada
	fig.add_trace(
		go.Scatter(
			x=monthly_totals["periodo"],
			y=total_bar,
			mode="text",
			text=[f"R$ {t:,.2f}" for t in total_bar],
			textposition="top center",
			showlegend=False,
			textfont={"size": 12, "color": "#fff", "family": "'Space Grotesk', sans-serif"},
			hoverinfo="skip",
			texttemplate="<span style='text-shadow: -2px -2px 0 #222, 2px -2px 0 #222, -2px 2px 0 #222, 2px 2px 0 #222;'>%{text}</span>",
		)
	)
	fig.update_layout(
		template="plotly_dark",
		title="Consumo Combustível por mês",
		xaxis_title="Período",
		yaxis_title="Valor faturado",
		margin={"l": 30, "r": 30, "t": 72, "b": 30},
		bargap=0.45,
		barmode="stack",
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





def make_donut_combustivel(df_filtered: pd.DataFrame) -> go.Figure:
	if df_filtered.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Sem dados por combustível")
		return apply_plotly_theme(fig)

	by_fuel = df_filtered.groupby("combustivel", as_index=False).agg(valor=("valor", "sum"))
	color_map = {"GASOLINA": "#2563eb", "ALCOOL": "#ff7f0e", "DIESEL": "#fb7185", "DIESEL S10": "#38bdf8"}
	fig = go.Figure(go.Pie(
		labels=by_fuel["combustivel"],
		values=by_fuel["valor"],
		hole=0.5,
		marker_colors=[color_map.get(c, "#2563eb") for c in by_fuel["combustivel"]],
		textinfo="label+percent",
		hoverinfo="label+value+percent",
		showlegend=True
	))
	fig.update_layout(
		template="plotly_dark",
		title="Consumo combustível por tipo",
		margin={"l": 20, "r": 20, "t": 50, "b": 80},
		legend={
			"font": {"size": 20, "color": "#f8fbff"},
			"bgcolor": "rgba(0,0,0,0)",
			"orientation": "h",
			"x": 0.5,
			"y": -0.15,
			"xanchor": "center",
			"yanchor": "top",
			"bordercolor": "#38bdf8",
			"borderwidth": 1
		},
	)
	return apply_plotly_theme(fig)



	consumo = consumo.sort_values("gasto_valor", ascending=False)

	# Definir cor: azul se dentro do limite, vermelho se acima
	cor_consumo = [
		"#2563eb" if (pd.notnull(lim) and val <= lim) else "#e63946"
		for val, lim in zip(consumo["gasto_valor"], consumo["limite_valor_periodo"])
	]

	fig = go.Figure()
	# Barra de consumo
	fig.add_trace(go.Bar(
		y=consumo["secretaria"],
		x=consumo["gasto_valor"],
		orientation="h",
		name="Consumo",
		marker_color=cor_consumo,
		text=[f"R$ {v:,.0f}" for v in consumo["gasto_valor"]],
		textposition="outside",
		textfont={"color": "#fff", "size": 16},
	))
	# Barra de limite
	if consumo["limite_valor_periodo"].notnull().any():
		fig.add_trace(go.Bar(
			y=consumo["secretaria"],
			x=consumo["limite_valor_periodo"],
			orientation="h",
			name="Limite",
			marker_color="#eab308",
			opacity=0.5,
			text=[f"R$ {v:,.0f}" if pd.notnull(v) else "" for v in consumo["limite_valor_periodo"]],
			textposition="inside",
			textfont={"color": "#222", "size": 14},
		))

	fig.update_layout(
		barmode="overlay",
		template="plotly_dark",
		title={"text": "Ranking de Consumo por Secretaria (com Limite)", "x": 0.01, "y": 0.98},
		margin={"l": 120, "r": 40, "t": 70, "b": 30},
		yaxis={"categoryorder": "total ascending"},
	)
	return apply_plotly_theme(fig)


def make_bullet_secretarias(status_df: pd.DataFrame) -> go.Figure:
	# Ordenar secretarias pelo percentual gasto do empenho (gasto_valor / empenho_2026)
	data = status_df.copy()
	data = data.sort_values("gasto_valor", ascending=True)
	fig = go.Figure()

	# Cálculos corretos usando limite_valor_periodo
	empenho = data["empenho_2026"]
	limite_periodo = data["limite_valor_periodo"]
	gasto = data["gasto_valor"]
	gasto_ate_limite = gasto.where(gasto <= limite_periodo, limite_periodo)
	excesso = (gasto - limite_periodo).clip(lower=0)
	saldo = (empenho - gasto).clip(lower=0)

	# Azul: gasto até limite
	fig.add_trace(
		go.Bar(
			y=data["secretaria"],
			x=gasto_ate_limite,
			orientation="h",
			name="Gasto até limite",
			marker_color="#2563eb",  # Azul igual ao gráfico de barras
			opacity=0.95,
			text=[f"R$ {v:,.0f}" if v > 0 else "" for v in gasto_ate_limite],
			textposition="inside",
			insidetextanchor="middle",
			textfont={"color": "#fff", "size": 12},
			hovertemplate="<b>%{y}</b><br>Gasto até limite: <b>R$ %{x:,.0f}</b><br>Limite: R$ %{customdata[0]:,.0f}<br>Empenho: R$ %{customdata[1]:,.0f}",
			customdata=list(zip(limite_periodo, empenho)),
		)
	)
	# Vermelho: excesso
	fig.add_trace(
		go.Bar(
			y=data["secretaria"],
			x=excesso,
			orientation="h",
			name="Excesso sobre limite",
			marker_color="#ef4444",
			opacity=0.95,
			text=[f"R$ {v:,.0f}" if v > 0 else "" for v in excesso],
			textposition="inside",
			insidetextanchor="middle",
			textfont={"color": "#fff", "size": 12},
			hovertemplate="<b>%{y}</b><br>Excesso: <b>R$ %{x:,.0f}</b><br>Limite: R$ %{customdata[0]:,.0f}<br>Empenho: R$ %{customdata[1]:,.0f}",
			customdata=list(zip(limite_periodo, empenho)),
		)
	)
	# Cinza: saldo do empenho
	fig.add_trace(
		go.Bar(
			y=data["secretaria"],
			x=saldo,
			orientation="h",
			name="Saldo do empenho",
			marker_color="#94a3b8",
			opacity=0.85,
			text=[f"R$ {v:,.0f}" if v > 0 else "" for v in saldo],
			textposition="inside",
			insidetextanchor="middle",
			textfont={"color": "#222", "size": 12},
			hovertemplate="<b>%{y}</b><br>Saldo do empenho: <b>R$ %{x:,.0f}</b><br>Empenho: R$ %{customdata[0]:,.0f}",
			customdata=list(zip(empenho)),
		)
	)

	# Após adicionar as barras, adicionar o valor do empenho fora das barras, alinhado à direita
	max_x = (gasto_ate_limite + excesso + saldo).max()
	deslocamento = max_x * 0.01 if max_x > 0 else 1
	for idx, (sec, emp, x_gasto, x_excesso, x_saldo) in enumerate(zip(data["secretaria"], empenho, gasto_ate_limite, excesso, saldo)):
		# Posição x: final da barra + deslocamento
		x_final = x_gasto + x_excesso + x_saldo + deslocamento
		fig.add_annotation(
			x=x_final,
			y=sec,
			text=f"Empenho: R$ {emp:,.0f}",
			showarrow=False,
			font=dict(size=12, color="#fff"),
			align="left",
			bgcolor="rgba(30,30,30,0.7)",
			bordercolor="#2563eb",
			borderwidth=1,
			borderpad=3,
			xanchor="left",
			yanchor="middle",
		)

	# Cálculos corretos usando limite_valor_periodo
	empenho = data["empenho_2026"]
	limite_periodo = data["limite_valor_periodo"]
	gasto = data["gasto_valor"]
	gasto_ate_limite = gasto.where(gasto <= limite_periodo, limite_periodo)
	excesso = (gasto - limite_periodo).clip(lower=0)
	saldo = (empenho - gasto).clip(lower=0)

	# Azul: gasto até limite
	fig.add_trace(
		go.Bar(
			y=data["secretaria"],
			x=gasto_ate_limite,
			orientation="h",
			name="Gasto até limite",
			marker_color="#2563eb",  # Azul igual ao gráfico de barras
			opacity=0.95,
			text=[f"R$ {v:,.0f}" if v > 0 else "" for v in gasto_ate_limite],
			textposition="inside",
			insidetextanchor="middle",
			textfont={"color": "#fff", "size": 12},
			hovertemplate="<b>%{y}</b><br>Gasto até limite: <b>R$ %{x:,.0f}</b><br>Limite: R$ %{customdata[0]:,.0f}<br>Empenho: R$ %{customdata[1]:,.0f}",
			customdata=list(zip(limite_periodo, empenho)),
		)
	)
	# Vermelho: excesso
	fig.add_trace(
		go.Bar(
			y=data["secretaria"],
			x=excesso,
			orientation="h",
			name="Excesso sobre limite",
			marker_color="#ef4444",
			opacity=0.95,
			text=[f"R$ {v:,.0f}" if v > 0 else "" for v in excesso],
			textposition="inside",
			insidetextanchor="middle",
			textfont={"color": "#fff", "size": 12},
			hovertemplate="<b>%{y}</b><br>Excesso: <b>R$ %{x:,.0f}</b><br>Limite: R$ %{customdata[0]:,.0f}<br>Empenho: R$ %{customdata[1]:,.0f}",
			customdata=list(zip(limite_periodo, empenho)),
		)
	)
	# Cinza: saldo do empenho
	fig.add_trace(
		go.Bar(
			y=data["secretaria"],
			x=saldo,
			orientation="h",
			name="Saldo do empenho",
			marker_color="#94a3b8",
			opacity=0.85,
			text=[f"R$ {v:,.0f}" if v > 0 else "" for v in saldo],
			textposition="inside",
			insidetextanchor="middle",
			textfont={"color": "#222", "size": 12},
			hovertemplate="<b>%{y}</b><br>Saldo do empenho: <b>R$ %{x:,.0f}</b><br>Empenho: R$ %{customdata[0]:,.0f}",
			customdata=list(zip(empenho)),
		)
	)

	# Adiciona o valor total do empenho ao lado direito das barras
	for idx, (sec, emp) in enumerate(zip(data["secretaria"], empenho)):
		fig.add_annotation(
			xref="paper",
			yref="y",
			x=1.01,
			y=sec,
			text=f"Empenho: R$ {emp:,.0f}",
			showarrow=False,
			font=dict(size=12, color="#fff"),
			align="left",
			bgcolor="rgba(30,30,30,0.7)",
			bordercolor="#2563eb",
			borderwidth=1,
			borderpad=3,
		)
	# Ordenar secretarias pelo percentual gasto do empenho (gasto_valor / empenho_2026)
	data = status_df.copy()
	data = data.sort_values("gasto_valor", ascending=True)
	fig = go.Figure()

	# Cálculos corretos usando limite_valor_periodo
	empenho = data["empenho_2026"]
	limite_periodo = data["limite_valor_periodo"]
	gasto = data["gasto_valor"]
	gasto_ate_limite = gasto.where(gasto <= limite_periodo, limite_periodo)
	excesso = (gasto - limite_periodo).clip(lower=0)
	saldo = (empenho - gasto).clip(lower=0)

	# Azul: gasto até limite
	fig.add_trace(
		go.Bar(
			y=data["secretaria"],
			x=gasto_ate_limite,
			orientation="h",
			name="Gasto até limite",
			marker_color="#2563eb",  # Azul igual ao gráfico de barras
			opacity=0.95,
			text=[f"R$ {v:,.0f}" if v > 0 else "" for v in gasto_ate_limite],
			textposition="inside",
			insidetextanchor="middle",
			textfont={"color": "#fff", "size": 12},
			hovertemplate="<b>%{y}</b><br>Gasto até limite: <b>R$ %{x:,.0f}</b><br>Limite: R$ %{customdata[0]:,.0f}<br>Empenho: R$ %{customdata[1]:,.0f}",
			customdata=list(zip(limite_periodo, empenho)),
		)
	)
	# Vermelho: excesso
	fig.add_trace(
		go.Bar(
			y=data["secretaria"],
			x=excesso,
			orientation="h",
			name="Excesso sobre limite",
			marker_color="#ef4444",
			opacity=0.95,
			text=[f"R$ {v:,.0f}" if v > 0 else "" for v in excesso],
			textposition="inside",
			insidetextanchor="middle",
			textfont={"color": "#fff", "size": 12},
			hovertemplate="<b>%{y}</b><br>Excesso: <b>R$ %{x:,.0f}</b><br>Limite: R$ %{customdata[0]:,.0f}<br>Empenho: R$ %{customdata[1]:,.0f}",
			customdata=list(zip(limite_periodo, empenho)),
		)
	)
	# Cinza: saldo do empenho
	fig.add_trace(
		go.Bar(
			y=data["secretaria"],
			x=saldo,
			orientation="h",
			name="Saldo do empenho",
			marker_color="#94a3b8",
			opacity=0.85,
			text=[f"R$ {v:,.0f}" if v > 0 else "" for v in saldo],
			textposition="inside",
			insidetextanchor="middle",
			textfont={"color": "#222", "size": 12},
			hovertemplate="<b>%{y}</b><br>Saldo do empenho: <b>R$ %{x:,.0f}</b><br>Empenho: R$ %{customdata[0]:,.0f}",
			customdata=list(zip(empenho)),
		)
	)

	# Adicionar linha de referência do limite para cada secretaria
	for idx, (sec, lim, gasto) in enumerate(zip(data["secretaria"], limite_periodo, gasto)):
		if pd.notnull(lim) and lim > 0:
			# Traço do limite sempre na altura da barra (até o valor máximo entre gasto e limite)
			x_max = max(lim, gasto)
			fig.add_shape(
				type="line",
				x0=lim,
				x1=lim,
				y0=idx - 0.35,
				y1=idx + 0.35,
				line={"color": "#eab308", "width": 4, "dash": "dash"},
				xref="x",
				yref="y",
				layer="above"
			)

	# Ajuste para visualização de empenhos pequenos: escala logarítmica se necessário
	use_log_x = (gasto_ate_limite.max() > 0 and gasto_ate_limite.min() > 0 and (gasto_ate_limite.max() / gasto_ate_limite.min() > 30))
	fig.update_layout(
		barmode="stack",
		template="plotly_dark",
		title={"text": "Grafico de bala: empenhado, gasto, excesso e saldo por secretaria", "x": 0.01, "y": 0.98},
		margin={"l": 120, "r": 40, "t": 92, "b": 30},
		legend={"orientation": "h", "x": 0.01, "y": 1.03, "xanchor": "left", "yanchor": "bottom"},
		shapes=fig.layout.shapes,
		bargap=0.25,  # Mais espaço entre barras
		height=max(600, 40 * len(data)),  # Altura dinâmica para barras mais altas
		xaxis={"type": "log" if use_log_x else "linear"},
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
				<div class="kpi-label">Média mensal de consumo</div>
				<div class="kpi-value">{currency(kpis['media_mensal_consumo'])}</div>
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
	st.plotly_chart(make_bar_gasto_por_mes(monthly_scope), use_container_width=True, key="bar_gasto_mes")
	st.plotly_chart(make_bar_consumo_tipo_mes(filtered), use_container_width=True, key="bar_combustivel")
	line_col, donut_col = st.columns([2, 1])
	line_col.plotly_chart(make_line_custo_medio_mes_combustivel(filtered), use_container_width=True, key="line_custo_medio_mes_combustivel")
	donut_col.plotly_chart(make_donut_combustivel(filtered), use_container_width=True, key="donut_combustivel")
	st.plotly_chart(
		make_line_real_previsto_projecao(
			filtered,
			limits_scope,
			usar_limite_quinzenal_secretaria=selected_secretaria != "Todas",
		),
		use_container_width=True,
	)
	st.plotly_chart(make_bar_consumo_secretaria(status, df_limits), use_container_width=True)

if __name__ == "__main__":
	run_dashboard()

