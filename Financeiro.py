import sqlite3
import datetime
import streamlit as st
import streamlit.components.v1 as st_components
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from plotly_utils import apply_plotly_theme
from make_bar_consumo_secretaria import make_bar_consumo_secretaria

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "relatorio.db"
DEFAULT_DISCOUNT_RATE = 0.0405


def _ensure_db_schema() -> None:
	"""Garante que todas as tabelas necessárias existam no banco de dados."""
	with sqlite3.connect(DB_PATH) as conn:
		conn.execute("""
			CREATE TABLE IF NOT EXISTS parametros_financeiros_anuais (
				secretaria TEXT NOT NULL,
				ano INTEGER NOT NULL,
				valor_empenhado REAL DEFAULT 0.0,
				limite_litros_gasolina REAL DEFAULT 0.0,
				limite_litros_alcool REAL DEFAULT 0.0,
				limite_litros_diesel REAL DEFAULT 0.0,
				desconto_percentual REAL DEFAULT 0.0,
				updated_at TEXT DEFAULT NULL,
				PRIMARY KEY (secretaria, ano)
			)
		""")
		# Migração: adiciona colunas que podem não existir em bancos mais antigos
		_colunas_existentes = {row[1] for row in conn.execute("PRAGMA table_info(parametros_financeiros_anuais)")}
		if 'updated_at' not in _colunas_existentes:
			conn.execute("ALTER TABLE parametros_financeiros_anuais ADD COLUMN updated_at TEXT DEFAULT NULL")
		conn.commit()


MONTHS = {
    1: "Janeiro",
    2: "Fevereiro",
    3: "Março",
    4: "Abril",
    5: "Maio",
    6: "Junho",
    7: "Julho",
    8: "Agosto",
    9: "Setembro",
    10: "Outubro",
    11: "Novembro",
    12: "Dezembro"
}
MONTH_NAME_TO_NUMBER = {v: k for k, v in MONTHS.items()}

FUEL_MAP = {
    "GASOLINA": "GASOLINA",
    "ALCOOL": "ALCOOL",
    "ETANOL": "ALCOOL",
    "DIESEL": "DIESEL",
    "DIESEL S10": "DIESEL S10",
}

def make_bar_gasto_por_mes_unificado(
	df_filtered: pd.DataFrame,
	selected_secretaria: str = "Todas",
	selected_combustivel: str = "Todos"
) -> go.Figure:
	"""
	Gráfico mensal sem duplicidade de meses, com labels corrigidos e dados agregados.
	Se selected_secretaria for diferente de 'Todas', a meta mensal será o empenho da secretaria filtrada dividido por 12.
	O filtro de combustível também é considerado na filtragem dos dados.
	"""
	value_mix, monthly_totals = build_monthly_mix(df_filtered)
	fig = go.Figure()
	if value_mix.empty or monthly_totals.empty:
		fig.update_layout(template="plotly_dark", title="Consumo por mês sem dados")
		return apply_plotly_theme(fig)

	# Corrigir nome do mês para 'março' com cedilha
	def corrige_mes_nome(periodo):
		partes = periodo.split('/')
		if len(partes) == 2 and partes[0].strip().lower() == 'marco':
			return 'Março/' + partes[1]
		return periodo

	# Calcular meta mensal considerando os anos presentes no filtro
	if "data_hora" in df_filtered.columns and not df_filtered.empty:
		_data_inicio = pd.to_datetime(df_filtered["data_hora"]).min().date()
		_data_fim = pd.to_datetime(df_filtered["data_hora"]).max().date()
		_limits_df = get_limits_df_for_period(_data_inicio, _data_fim)
	else:
		_limits_df = get_limits_df()

	if selected_secretaria and selected_secretaria != "Todas":
		_limits_df = _limits_df[_limits_df["secretaria"].str.upper() == selected_secretaria.upper()].copy()

	if _limits_df.empty:
		meta_mensal = 0.0
	elif "ano" in _limits_df.columns and "data_hora" in df_filtered.columns and not df_filtered.empty:
		# Meta mensal global: soma dos limites mensais por secretaria, ponderada por meses de cada ano.
		_df_aux = df_filtered.copy()
		_df_aux["ano"] = pd.to_datetime(_df_aux["data_hora"]).dt.year
		_df_aux["ano_mes"] = pd.to_datetime(_df_aux["data_hora"]).dt.to_period("M")
		months_by_year = _df_aux.groupby("ano")["ano_mes"].nunique().to_dict()

		limits_by_year = _limits_df.groupby("ano", as_index=False)["limite_mensal"].sum()
		weighted_sum = 0.0
		total_months = 0
		for _, _row in limits_by_year.iterrows():
			_ano = int(_row["ano"])
			_meses = int(months_by_year.get(_ano, 0))
			weighted_sum += float(_row.get("limite_mensal", 0.0)) * _meses
			total_months += _meses
		meta_mensal = weighted_sum / total_months if total_months > 0 else float(limits_by_year["limite_mensal"].sum())
	else:
		meta_mensal = float(_limits_df["limite_mensal"].sum()) if "limite_mensal" in _limits_df.columns else 0.0

	# Corrigir todos os labels e agregar valores por mês/ano único
	monthly_totals["periodo_corrigido"] = monthly_totals["periodo"].apply(corrige_mes_nome)
	agrupado = monthly_totals.groupby(["ano", "mes", "periodo_corrigido"], as_index=False).agg({
		"valor_total_mes": "sum",
		"litros_total_mes": "sum",
		"variacao_pct": "first"  # ou média, se preferir
	})
	# Recalcular azul/vermelho após agregação
	if meta_mensal > 0:
		agrupado["azul"] = agrupado["valor_total_mes"].clip(upper=meta_mensal)
		agrupado["vermelho"] = (agrupado["valor_total_mes"] - meta_mensal).clip(lower=0)
	else:
		# Sem parâmetros financeiros configurados: exibe tudo como azul
		agrupado["azul"] = agrupado["valor_total_mes"]
		agrupado["vermelho"] = 0.0
	# Customdata: variacao, litros, meta, excesso
	customdata = list(
		zip(
			agrupado["variacao_pct"],
			agrupado["litros_total_mes"],
			[meta_mensal]*len(agrupado),
			agrupado["vermelho"],
		)
	)
	# Função para formatar o valor excedente no hover azul
	def hover_excedente_str(excedente):
		if excedente > 0:
			return f"Excedeu: {excedente:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
		return ""

	fig.add_trace(
		go.Bar(
			x=agrupado["periodo_corrigido"],
			y=agrupado["azul"],
			name="Consumo até a meta",
			marker={"color": "#2563eb", "line": {"color": "#102a56", "width": 1.5}},
			text=None,
			customdata=[
				(
					v[0], v[1], v[2], hover_excedente_str(v[3])
				) for v in customdata
			],
			hovertemplate=(
				"<b>%{x}</b><br>"
				"Variação mensal: %{customdata[0]:+.2f}%<br>"
				"Litros: %{customdata[1]:,.0f}<br>"
				"Meta mensal: R$ %{customdata[2]:,.2f}" +
				"<br>%{customdata[3]}" +
				"<extra></extra>"
			),
		)
	)
	fig.add_trace(
		go.Bar(
			x=agrupado["periodo_corrigido"],
			y=agrupado["vermelho"],
			name="Excesso sobre a meta",
			marker={"color": "#e63946", "line": {"color": "#102a56", "width": 1.5}},
			text=None,
			showlegend=True,
			customdata=customdata,
			hovertemplate=(
				"<b>%{x}</b><br>"
				"Excedeu: %{y:,.2f}<br>"
				"Variação mensal: %{customdata[0]:+.2f}%<br>"
				"Litros: %{customdata[1]:,.0f}<br>"
				"Meta mensal: R$ %{customdata[2]:,.2f}<extra></extra>"
			),
		)
	)
	# Adiciona o valor total no topo da barra empilhada
	total_bar = agrupado["valor_total_mes"].tolist()
	fig.add_trace(
		go.Scatter(
			x=agrupado["periodo_corrigido"],
			y=total_bar,
			mode="text",
			text=[currency(t) for t in total_bar],
			textposition="top center",
			showlegend=False,
			textfont={"size": 12, "color": "#fff", "family": "'Space Grotesk', sans-serif"},
			hoverinfo="skip",
			texttemplate="<span style='text-shadow: -2px -2px 0 #222, 2px -2px 0 #222, -2px 2px 0 #222, 2px 2px 0 #222;'>%{text}</span>",
		)
	)
	# Limite mensal já calculado como meta_mensal
	limite_mensal = meta_mensal
	media_consumo = float(agrupado["valor_total_mes"].mean()) if not agrupado.empty else 0.0
	n_bars = len(agrupado)

	# Adiciona linha tracejada horizontal do limite (vermelho)
	if limite_mensal > 0:
		fig.add_shape(
			type="line",
			x0=-0.5,
			x1=n_bars - 0.5,
			y0=limite_mensal,
			y1=limite_mensal,
			line=dict(color="#ef4444", width=3, dash="dash"),
			xref="x",
			yref="y",
			layer="above"
		)
		# Scatter invisível para a legenda
		fig.add_trace(
			go.Scatter(
				x=[None],
				y=[None],
				mode="lines",
				line=dict(color="#ef4444", width=3, dash="dash"),
				name=f"Limite mensal ({currency(limite_mensal)})"
			)
		)

	# Adiciona linha tracejada horizontal da média de consumo (amarelo)
	if media_consumo > 0:
		fig.add_shape(
			type="line",
			x0=-0.5,
			x1=n_bars - 0.5,
			y0=media_consumo,
			y1=media_consumo,
			line=dict(color="#eab308", width=2, dash="dot"),
			xref="x",
			yref="y",
			layer="above"
		)
		fig.add_trace(
			go.Scatter(
				x=[None],
				y=[None],
				mode="lines",
				line=dict(color="#eab308", width=2, dash="dot"),
				name=f"Média de consumo ({currency(media_consumo)})"
			)
		)

	fig.update_layout(
		template="plotly_dark",
		title="Consumo Combustível por mês",
		xaxis_title="Período",
		yaxis_title="Valor faturado",
		margin={"l": 30, "r": 30, "t": 48, "b": 60},
		bargap=0.45,
		barmode="stack",
		legend={"orientation": "h", "x": 0.5, "y": -0.18, "xanchor": "center", "yanchor": "top"},
	)
	return apply_plotly_theme(fig)


def make_bar_gasto_por_ano(df: pd.DataFrame, selected_secretaria: str = "Todas", selected_combustivel: str = "Todos", discount_rate: float = 0.0) -> go.Figure:
    if df.empty or not {'ano', 'valor'}.issubset(df.columns):
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados para gasto anual")
        return apply_plotly_theme(fig)

    grupo = df.groupby("ano", as_index=False).agg(valor_total=("valor", "sum"))
    grupo = grupo.sort_values("ano")
    
    # Calcular valor bruto (antes do desconto) dividindo pelo fator de desconto
    grupo["valor_bruto"] = grupo["valor_total"] / (1 - discount_rate) if discount_rate > 0 else grupo["valor_total"]
    grupo["valor_desconto"] = grupo["valor_bruto"] - grupo["valor_total"]
    
    # Paleta fixa por ano dentro do espectro azul (mais claro = mais antigo, mais escuro = atual)
    PALETA_ANOS = {
        2022: "#bfdbfe",  # azul muito claro
        2023: "#7dd3fc",  # azul claro
        2024: "#38bdf8",  # azul médio-claro (sky)
        2025: "#3b82f6",  # azul médio
        2026: "#1d4ed8",  # azul escuro (atual)
    }
    ano_atual = int(grupo["ano"].max())
    
    fig = go.Figure(go.Bar(
        x=grupo["ano"].astype(str),
        y=grupo["valor_total"],
        marker_color=[PALETA_ANOS.get(int(a), "#60a5fa") for a in grupo["ano"]],
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
    
    # Adicionar subtítulo informando sobre o desconto
    
    fig.update_layout(
        template="plotly_dark",
        title="Gasto anual comparativo",
        xaxis_title="Ano",
        yaxis_title="Valor total (R$)",
        margin={"l": 20, "r": 20, "t": 48, "b": 30},
        legend={"font": {"size": 13, "color": "#eaf2ff"}},
    )
    return apply_plotly_theme(fig)


def make_donut_combustivel(df_filtered: pd.DataFrame) -> go.Figure:
	if df_filtered.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Sem dados por combustível")
		return apply_plotly_theme(fig)

	by_fuel = df_filtered.groupby("combustivel", as_index=False).agg(litros=("litros", "sum"))
	color_map = {"GASOLINA": "#2563eb", "ALCOOL": "#22c55e", "ETANOL": "#22c55e", "DIESEL": "#f97316", "DIESEL S10": "#f97316"}
	fig = go.Figure(go.Pie(
		labels=by_fuel["combustivel"],
		values=by_fuel["litros"],
		hole=0.5,
		marker_colors=[color_map.get(c, "#2563eb") for c in by_fuel["combustivel"]],
		textinfo="label+percent",
		hovertemplate="<b>%{label}</b><br>%{value:,.0f} L<br>%{percent}<extra></extra>",
		showlegend=True
	))
	fig.update_layout(
		template="plotly_dark",
		title="Combustível por volume (Litros)",
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


def make_donut_combustivel_valor(df_filtered: pd.DataFrame) -> go.Figure:
	if df_filtered.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Sem dados por combustível")
		return apply_plotly_theme(fig)

	by_fuel = df_filtered.groupby("combustivel", as_index=False).agg(valor=("valor", "sum"))
	color_map = {"GASOLINA": "#2563eb", "ALCOOL": "#22c55e", "ETANOL": "#22c55e", "DIESEL": "#f97316", "DIESEL S10": "#f97316"}
	fig = go.Figure(go.Pie(
		labels=by_fuel["combustivel"],
		values=by_fuel["valor"],
		hole=0.5,
		marker_colors=[color_map.get(c, "#2563eb") for c in by_fuel["combustivel"]],
		textinfo="label+percent",
		hovertemplate="<b>%{label}</b><br>R$ %{value:,.2f}<br>%{percent}<extra></extra>",
		showlegend=True
	))
	fig.update_layout(
		template="plotly_dark",
		title="Combustível por valor (R$)",
		margin={"l": 20, "r": 20, "t": 50, "b": 80},
		legend={
			"font": {"size": 14, "color": "#f8fbff"},
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
	st_components.html(
		"""
		<script>
		(function() {
			// Set language attributes
			var r = window.parent.document.documentElement;
			r.setAttribute('translate', 'no');
			r.setAttribute('lang', 'pt-BR');
			var m = window.parent.document.createElement('meta');
			m.name = 'google'; m.content = 'notranslate';
			window.parent.document.head.appendChild(m);

			// Force dark background immediately to prevent white flash
			document.documentElement.style.backgroundColor = '#0a121b';
			document.body.style.backgroundColor = '#0a121b';
			if (window.parent.document) {
				window.parent.document.documentElement.style.backgroundColor = '#0a121b';
				window.parent.document.body.style.backgroundColor = '#0a121b';
			}
		})();
		</script>
		""",
		height=0,
	)
	st.markdown(
		"""
		<style>
		@import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;700&family=Space+Grotesk:wght@400;500;700&display=swap');

		/* ── Elimina flash branco durante transições ── */
		html, body {
			background-color: #0a121b !important;
			background-image: radial-gradient(circle at 20% -10%, rgba(56,189,248,0.20), transparent 35%), radial-gradient(circle at 90% 0%, rgba(45,212,191,0.16), transparent 30%) !important;
		}

		iframe {
			background-color: #0a121b !important;
		}

		/* ── Transições suaves ── */
		* {
			transition: background-color 0.15s ease, color 0.15s ease !important;
		}

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
			min-height: 100vh;
			background-attachment: fixed;
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
			display: flex;
			flex-direction: row;
			gap: 12px;
			margin-bottom: 8px;
			justify-content: space-between;
			flex-wrap: nowrap;
			max-width: 100vw;
		}

		.kpi-card {
			background: linear-gradient(150deg, #162436 0%, #111d2b 100%);
			border: 1px solid rgba(142,163,190,0.22);
			border-radius: 12px;
			padding: 10px 18px 10px 18px;
			box-shadow: 0 6px 16px rgba(0, 0, 0, 0.22);
			min-width: 210px;
			max-width: 260px;
			display: flex;
			flex-direction: column;
			align-items: flex-start;
			justify-content: center;
			white-space: nowrap;
			overflow: hidden;
			text-overflow: ellipsis;
		}

		.kpi-label {
			font-size: 1rem;
			color: #8ea3be;
			letter-spacing: 0.04em;
			margin-bottom: 2px;
			white-space: nowrap;
			overflow: hidden;
			text-overflow: ellipsis;
		}

		.kpi-value {
			font-family: 'Rajdhani', sans-serif;
			font-size: 1.35rem;
			line-height: 1.1;
			font-weight: 700;
			color: #e7eef8;
			white-space: nowrap;
			overflow: hidden;
			text-overflow: ellipsis;
		}

		.section-title {
			font-family: 'Rajdhani', 'Space Grotesk', sans-serif;
			font-size: 1.5rem;
			font-weight: 700;
			letter-spacing: 0.02em;
			margin: 1.1rem 0 0.15rem 0;
			padding-bottom: 0.3rem;
			border-bottom: 1px solid rgba(56,189,248,0.18);
			color: #e7eef8;
		}

		.js-plotly-plot .gtitle, .js-plotly-plot .gtitle-main, .js-plotly-plot .gtitle-txt {
			font-size: 1.35rem !important;
			font-family: 'Space Grotesk', 'Rajdhani', sans-serif !important;
			font-weight: 700 !important;
			color: #e7eef8 !important;
		}

		div[data-testid="stDataFrame"] {
			border: 1px solid rgba(142,163,190,0.20);
			border-radius: 10px;
			overflow: hidden;
		}

		@media (max-width: 1100px) {
			.kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
		}

		/* ── Sidebar brand ── */
		.sidebar-brand {
			display: flex;
			align-items: center;
			gap: 14px;
			padding: 8px 0 14px 0;
			border-bottom: 1px solid rgba(142,163,190,0.18);
			margin-bottom: 6px;
		}
		.sidebar-brand-icon {
			font-size: 2rem;
			line-height: 1;
		}
		.sidebar-brand-title {
			font-family: 'Rajdhani', sans-serif;
			font-size: 1.15rem;
			font-weight: 700;
			color: #e7eef8;
			line-height: 1.1;
			letter-spacing: 0.04em;
		}
		.sidebar-brand-sub {
			font-size: 0.7rem;
			color: #8ea3be;
			text-transform: uppercase;
			letter-spacing: 0.1em;
		}

		/* ── Sidebar section header ── */
		.sidebar-section {
			font-size: 0.68rem;
			font-weight: 700;
			letter-spacing: 0.14em;
			color: #8ea3be;
			text-transform: uppercase;
			margin: 14px 0 8px 0;
			padding-bottom: 4px;
			border-bottom: 1px solid rgba(142,163,190,0.18);
		}

		/* ── Alert cards ── */
		.alert-card {
			display: flex;
			align-items: center;
			gap: 12px;
			padding: 9px 12px;
			border-radius: 8px;
			margin-bottom: 7px;
			background: rgba(10, 18, 27, 0.85);
			border: 1px solid rgba(142,163,190,0.12);
		}
		.alert-card.alert-red  { border-left: 4px solid #ef4444; }
		.alert-card.alert-yellow { border-left: 4px solid #eab308; }
		.alert-card.alert-green  { border-left: 4px solid #22c55e; }
		.alert-icon { font-size: 1.4rem; line-height: 1; }
		.alert-body { display: flex; flex-direction: column; gap: 1px; }
		.alert-count-red    { font-family:'Rajdhani',sans-serif; font-size:1.3rem; font-weight:700; color:#ef4444; line-height:1; }
		.alert-count-yellow { font-family:'Rajdhani',sans-serif; font-size:1.3rem; font-weight:700; color:#eab308; line-height:1; }
		.alert-count-green  { font-family:'Rajdhani',sans-serif; font-size:1.3rem; font-weight:700; color:#22c55e; line-height:1; }
		.alert-label { font-size: 0.75rem; color: #c9d8ea; }
		.alert-sublabel { font-size: 0.62rem; color: #7a95b0; font-style: italic; }
		.alert-detail { font-size: 0.65rem; color: #8ea3be; line-height: 1.5; }

		/* ── Última atualização ── */
		.sidebar-footer {
			font-size: 0.68rem;
			color: #8ea3be;
			margin-top: 10px;
			padding-top: 8px;
			border-top: 1px solid rgba(142,163,190,0.15);
		}

		.kpi-delta {
			font-size: 0.75rem;
			font-weight: 600;
			margin-top: 4px;
			border-radius: 5px;
			padding: 1px 8px;
			display: inline-block;
			white-space: nowrap;
			letter-spacing: 0.03em;
		}
		</style>
		""",
		unsafe_allow_html=True,
	)


def render_kpi_cards(kpis: dict[str, float | str], deltas: dict | None = None) -> None:
		def _fmt_num(v, dec=0):
			fmt = f"{v:,.{dec}f}".replace(",", "X").replace(".", ",").replace("X", ".")
			return fmt

		def _delta_badge(key, inverted=False, neutral=False):
			"""Retorna HTML do badge YoY ou string vazia se não disponível."""
			if not deltas or key not in deltas:
				return ""
			pct, ano_ref = deltas[key]
			if pct is None:
				return ""
			# Suprime badges com variação insignificante (ruído visual)
			if abs(pct) < 0.5:
				return ""
			is_up = pct > 0
			arrow = "▲" if is_up else "▼"
			if neutral:
				color = "#94a3b8"
				bg = "rgba(148,163,184,0.15)"
			else:
				is_bad = is_up if inverted else not is_up
				color = "#f87171" if is_bad else "#4ade80"
				bg = "rgba(239,68,68,0.15)" if is_bad else "rgba(34,197,94,0.15)"
			return f'<div class="kpi-delta" style="color:{color};background:{bg};">{arrow} {abs(pct):.1f}% vs {ano_ref}</div>'

		html = f"""
		<div class="kpi-grid">
			<div class="kpi-card">
				<div class="kpi-label">{kpis['label_valor_empenhado']}</div>
				<div class="kpi-value">{currency(kpis['valor_empenhado'])}</div>
				{_delta_badge('valor_empenhado', neutral=True)}
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
		<div class="kpi-grid" style="margin-top:8px;">
			<div class="kpi-card">
				<div class="kpi-label">Total de Litros</div>
				<div class="kpi-value">{_fmt_num(kpis['gasto_litros'], 2)} L</div>
				{_delta_badge('gasto_litros', inverted=True)}
			</div>
			<div class="kpi-card">
				<div class="kpi-label">Consumo Médio</div>
				<div class="kpi-value">{_fmt_num(kpis['consumo_medio'], 2)} km/L</div>
				{_delta_badge('consumo_medio', inverted=False)}
			</div>
			<div class="kpi-card">
				<div class="kpi-label">Custo Médio (R$/km)</div>
				<div class="kpi-value">{currency(kpis['custo_por_km'])}</div>
				{_delta_badge('custo_por_km', inverted=True)}
			</div>
			<div class="kpi-card">
				<div class="kpi-label">Nº Abastecimentos</div>
				<div class="kpi-value">{_fmt_num(kpis['n_abastecimentos'])}</div>
				{_delta_badge('n_abastecimentos', neutral=True)}
			</div>
			<div class="kpi-card">
				<div class="kpi-label">Veículos Ativos</div>
				<div class="kpi-value">{kpis['veiculos_ativos']}</div>
				{_delta_badge('veiculos_ativos', inverted=False)}
			</div>
		</div>
		"""
		# Remove indentação de tabs — em Markdown, linhas iniciadas com tab são
		# interpretadas como blocos de código, o que faz o HTML aparecer como texto cru.
		html_clean = "\n".join(line.lstrip() for line in html.splitlines())
		st.markdown(html_clean, unsafe_allow_html=True)


def currency(value: float) -> str:
	return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


@st.cache_data(show_spinner=False, ttl=60)
def get_financial_params_by_years(anos: list[int], secretaria: str | None = None) -> pd.DataFrame:
	"""
	Lê parâmetros financeiros (empenho, limites, desconto) do banco para anos e opcionalmente secretaria específicas.
	Se secretaria for None, traz todas.
	Se não houver cadastro para um ano/secretaria, deixa zerado (não filtra out).
	"""
	with sqlite3.connect(DB_PATH) as conn:
		query = """
			SELECT 
				secretaria,
				ano,
				valor_empenhado,
				limite_litros_gasolina,
				limite_litros_alcool,
				limite_litros_diesel,
				desconto_percentual
			FROM parametros_financeiros_anuais
			WHERE ano IN ({})
		""".format(','.join('?' * len(anos)))
		
		params = anos
		if secretaria:
			query += " AND secretaria = ?"
			params = anos + [secretaria]
		
		query += " ORDER BY secretaria, ano"
		df = pd.read_sql_query(query, conn, params=params)
	
	# Garantir que não haja NaN nos valores numéricos
	for col in ['valor_empenhado', 'limite_litros_gasolina', 'limite_litros_alcool', 'limite_litros_diesel', 'desconto_percentual']:
		if col in df.columns:
			df[col] = df[col].fillna(0.0)
	
	# Derivar colunas necessárias
	df['limite_mensal'] = df['valor_empenhado'] / 12.0
	df['limite_litros_mensal'] = df['limite_litros_gasolina'] + df['limite_litros_alcool'] + df['limite_litros_diesel']
	
	return df


@st.cache_data(show_spinner=False)
def get_limits_df() -> pd.DataFrame:
	"""
	Retorna parâmetros financeiros para o ano corrente (compatibilidade com código legado).
	De preferência, usar get_financial_params_by_years() em novo código.
	"""
	current_year = datetime.date.today().year
	df = get_financial_params_by_years([current_year])
	return df


@st.cache_data(show_spinner=False)
def get_discount_rate() -> float:
	"""
	Retorna taxa de desconto para o ano corrente (compatibilidade com código legado).
	De preferência, extrair do dataframe de parâmetros em novo código.
	"""
	current_year = datetime.date.today().year
	df = get_financial_params_by_years([current_year])
	if df.empty:
		return DEFAULT_DISCOUNT_RATE
	# Pega o primeiro valor de desconto disponível (normalmente todos iguais no mesmo ano)
	return float(df['desconto_percentual'].iloc[0])


def get_financial_params_editor_df() -> pd.DataFrame:
	"""Carrega todos os parâmetros financeiros anuais para edição na sidebar."""
	with sqlite3.connect(DB_PATH) as conn:
		df = pd.read_sql_query(
			"""
			SELECT
				secretaria,
				ano,
				valor_empenhado,
				limite_litros_gasolina,
				limite_litros_alcool,
				limite_litros_diesel,
				desconto_percentual
			FROM parametros_financeiros_anuais
			ORDER BY secretaria, ano
			""",
			conn,
		)

	if df.empty:
		return pd.DataFrame(
			columns=[
				"secretaria",
				"ano",
				"valor_empenhado",
				"limite_litros_gasolina",
				"limite_litros_alcool",
				"limite_litros_diesel",
				"desconto_percentual",
			]
		)

	for col in [
		"valor_empenhado",
		"limite_litros_gasolina",
		"limite_litros_alcool",
		"limite_litros_diesel",
		"desconto_percentual",
	]:
		df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
	df["ano"] = pd.to_numeric(df["ano"], errors="coerce").fillna(0).astype(int)
	df["secretaria"] = df["secretaria"].astype(str).str.strip().str.upper()
	return df


def save_financial_params_editor_df(df_editor: pd.DataFrame) -> tuple[int, int]:
	"""Persiste alterações do editor aplicando insert/update/delete por (secretaria, ano)."""
	cols = [
		"secretaria",
		"ano",
		"valor_empenhado",
		"limite_litros_gasolina",
		"limite_litros_alcool",
		"limite_litros_diesel",
		"desconto_pct",
	]
	missing = [c for c in cols if c not in df_editor.columns]
	if missing:
		raise ValueError(f"Colunas obrigatórias ausentes: {', '.join(missing)}")

	df = df_editor[cols].copy()
	df["secretaria"] = df["secretaria"].astype(str).str.strip().str.upper()
	df["ano"] = pd.to_numeric(df["ano"], errors="coerce")

	for col in [
		"valor_empenhado",
		"limite_litros_gasolina",
		"limite_litros_alcool",
		"limite_litros_diesel",
		"desconto_pct",
	]:
		df[col] = pd.to_numeric(df[col], errors="coerce")

	# Remove linhas completamente vazias criadas dinamicamente no editor.
	empty_row = (
		(df["secretaria"] == "")
		& df["ano"].isna()
		& df[["valor_empenhado", "limite_litros_gasolina", "limite_litros_alcool", "limite_litros_diesel", "desconto_pct"]].isna().all(axis=1)
	)
	df = df[~empty_row].copy()

	if df["secretaria"].eq("").any():
		raise ValueError("A coluna secretaria não pode ficar vazia.")
	if df["ano"].isna().any():
		raise ValueError("A coluna ano deve ser preenchida para todas as linhas.")

	df["ano"] = df["ano"].astype(int)
	if ((df["ano"] < 2000) | (df["ano"] > 2100)).any():
		raise ValueError("Ano inválido. Use valores entre 2000 e 2100.")

	for col in ["valor_empenhado", "limite_litros_gasolina", "limite_litros_alcool", "limite_litros_diesel", "desconto_pct"]:
		df[col] = df[col].fillna(0.0)
		if (df[col] < 0).any():
			raise ValueError(f"Valores negativos não são permitidos em {col}.")

	if (df["desconto_pct"] > 100).any():
		raise ValueError("Desconto (%) deve ficar entre 0 e 100.")

	dupes = df.duplicated(subset=["secretaria", "ano"], keep=False)
	if dupes.any():
		keys = [f"{row.secretaria}/{row.ano}" for row in df.loc[dupes, ["secretaria", "ano"]].itertuples(index=False)]
		raise ValueError("Existem chaves duplicadas (secretaria/ano): " + ", ".join(sorted(set(keys))))

	df["desconto_percentual"] = (df["desconto_pct"] / 100.0).astype(float)

	with sqlite3.connect(DB_PATH) as conn:
		current_keys = set(conn.execute("SELECT secretaria, ano FROM parametros_financeiros_anuais").fetchall())
		new_keys = {(row.secretaria, int(row.ano)) for row in df[["secretaria", "ano"]].itertuples(index=False)}
		to_delete = current_keys - new_keys

		for row in df.itertuples(index=False):
			conn.execute(
				"""
				INSERT INTO parametros_financeiros_anuais (
					secretaria,
					ano,
					valor_empenhado,
					limite_litros_gasolina,
					limite_litros_alcool,
					limite_litros_diesel,
					desconto_percentual,
					updated_at
				)
				VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
				ON CONFLICT(secretaria, ano) DO UPDATE SET
					valor_empenhado = excluded.valor_empenhado,
					limite_litros_gasolina = excluded.limite_litros_gasolina,
					limite_litros_alcool = excluded.limite_litros_alcool,
					limite_litros_diesel = excluded.limite_litros_diesel,
					desconto_percentual = excluded.desconto_percentual,
					updated_at = CURRENT_TIMESTAMP
				""",
				(
					row.secretaria,
					int(row.ano),
					float(row.valor_empenhado),
					float(row.limite_litros_gasolina),
					float(row.limite_litros_alcool),
					float(row.limite_litros_diesel),
					float(row.desconto_percentual),
				),
			)

		if to_delete:
			conn.executemany(
				"DELETE FROM parametros_financeiros_anuais WHERE secretaria = ? AND ano = ?",
				list(to_delete),
			)

		conn.commit()

	return len(df), len(to_delete)


@st.dialog("Parâmetros Financeiros Anuais", width="large")
def render_financial_params_editor_modal() -> None:
	"""Modal com pesquisa por ano e edição dos parâmetros financeiros anuais."""
	df_params = get_financial_params_editor_df().copy()
	df_params["desconto_pct"] = pd.to_numeric(df_params.get("desconto_percentual", 0.0), errors="coerce").fillna(0.0) * 100.0
	df_params = df_params[
		[
			"secretaria",
			"ano",
			"valor_empenhado",
			"limite_litros_gasolina",
			"limite_litros_alcool",
			"limite_litros_diesel",
			"desconto_pct",
		]
	]

	years = sorted(int(v) for v in df_params["ano"].dropna().unique().tolist()) if not df_params.empty else []
	ano_options = ["Todos"] + years
	selected_ano = st.selectbox("Pesquisar por ano", options=ano_options, index=0, key="editor_parametros_ano_busca")

	if selected_ano == "Todos":
		editor_df = df_params.copy()
	else:
		editor_df = df_params[df_params["ano"] == int(selected_ano)].copy()

	st.caption("Edite os valores, inclua novas linhas ou apague linhas. Clique em Salvar para aplicar.")
	edited = st.data_editor(
		editor_df,
		use_container_width=True,
		height=480,
		hide_index=True,
		num_rows="dynamic",
		key=f"editor_parametros_financeiros_{selected_ano}",
		column_config={
			"secretaria": st.column_config.TextColumn("Secretaria", required=True),
			"ano": st.column_config.NumberColumn("Ano", min_value=2000, max_value=2100, step=1),
			"valor_empenhado": st.column_config.NumberColumn("Valor empenhado (R$)", min_value=0.0, format="%.2f"),
			"limite_litros_gasolina": st.column_config.NumberColumn("Limite gasolina (L)", min_value=0.0, format="%.2f"),
			"limite_litros_alcool": st.column_config.NumberColumn("Limite álcool (L)", min_value=0.0, format="%.2f"),
			"limite_litros_diesel": st.column_config.NumberColumn("Limite diesel (L)", min_value=0.0, format="%.2f"),
			"desconto_pct": st.column_config.NumberColumn("Desconto (%)", min_value=0.0, max_value=100.0, format="%.2f"),
		},
	)

	btn_save, btn_close = st.columns(2)
	if btn_save.button("💾 Salvar", use_container_width=True, type="primary", key="btn_save_params_modal"):
		try:
			if selected_ano == "Todos":
				df_to_save = edited.copy()
			else:
				df_other_years = df_params[df_params["ano"] != int(selected_ano)].copy()
				df_to_save = pd.concat([df_other_years, edited], ignore_index=True)

			n_rows, n_deleted = save_financial_params_editor_df(df_to_save)
		except Exception as exc:
			st.error(f"Não foi possível salvar: {exc}")
		else:
			get_financial_params_by_years.clear()
			get_limits_df.clear()
			get_discount_rate.clear()
			st.session_state["show_fin_params_modal"] = False
			st.toast(f"Parâmetros salvos. Linhas ativas: {n_rows}. Removidas: {n_deleted}.")
			st.rerun()

	if btn_close.button("Fechar", use_container_width=True, key="btn_close_params_modal"):
		st.session_state["show_fin_params_modal"] = False
		st.rerun()


def render_financial_params_editor_sidebar() -> None:
	"""Botão no menu lateral que abre modal de edição de parâmetros financeiros."""
	if "show_fin_params_modal" not in st.session_state:
		st.session_state["show_fin_params_modal"] = False

	if st.button("⚙️ Editar parâmetros financeiros", use_container_width=True, key="btn_open_fin_params_modal"):
		st.session_state["show_fin_params_modal"] = True
		st.rerun()

	if st.session_state.get("show_fin_params_modal", False):
		render_financial_params_editor_modal()


@st.cache_data(show_spinner=False)
def get_real_df(db_version: int = 0) -> pd.DataFrame:
	_ = db_version
	return load_sqlite(DB_PATH)




def normalize_secretaria(value: str) -> str:
	normalized = str(value or "").strip().upper()
	secretaria_aliases = {
		"SEMUTTRANS": "SMTT",
		"SEMUTRANS": "SMTT",
	}
	return secretaria_aliases.get(normalized, normalized)


def normalize_fuel(value: str) -> str:
    raw = str(value or "").strip().upper()
    return FUEL_MAP.get(raw, raw)


def clamp_discount_rate(value: float) -> float:
    return min(max(float(value), 0.0), 1.0)


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
                "Data/Hora"            AS data_hora,
                "Unidade"              AS secretaria,
                "Produto"              AS combustivel,
                "Vr. Unit."            AS valor_unitario,
                "Qtde (L)"             AS litros,
                "Valor"                AS valor,
                "Placa"                AS placa,
                "Condutor"             AS condutor,
                "Km Rodado"            AS km_rodado,
                "km/L"                 AS km_por_litro,
                "R$/km"                AS custo_por_km,
                "KM Minimo"            AS km_minimo,
                "KM Maximo"            AS km_maximo,
                "Estabelecimento"      AS posto,
                "Marca"                AS marca,
                "Modelo"               AS modelo,
                "Tipo Frota"           AS tipo_frota
            FROM {table_name}
        """
        df = pd.read_sql_query(query, conn)

    df["data_hora"] = pd.to_datetime(df["data_hora"], errors="coerce")
    df["secretaria"] = df["secretaria"].map(normalize_secretaria)
    df["combustivel"] = df["combustivel"].map(normalize_fuel)
    for num_col in ("valor_unitario", "litros", "valor", "km_rodado", "km_por_litro", "custo_por_km", "km_minimo", "km_maximo"):
        if num_col in df.columns:
            df[num_col] = pd.to_numeric(df[num_col], errors="coerce").fillna(0.0)
    for str_col in ("placa", "condutor", "posto", "marca", "modelo", "tipo_frota"):
        if str_col in df.columns:
            df[str_col] = df[str_col].astype(str).str.strip()
    if "placa" in df.columns:
        df["placa"] = df["placa"].str.upper()
    df = df.dropna(subset=["data_hora"])
    df["ano"] = df["data_hora"].dt.year
    df["mes"] = df["data_hora"].dt.month
    df["mes_nome"] = df["mes"].map(MONTHS)
    df["ano_mes"] = df["data_hora"].dt.to_period("M").astype(str)
    return df


def month_count(df: pd.DataFrame) -> int:
    if df.empty:
        return 1
    return max(1, int(df["ano_mes"].nunique()))


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


def build_secretaria_status(df_filtered: pd.DataFrame, df_limits: pd.DataFrame) -> pd.DataFrame:
	"""
	Calcula status de secretarias (gasto vs limite) para o período filtrado.
	df_limits pode ter múltiplas linhas por secretaria (uma por ano), e esta função
	agrega o limite considerando quantos meses de cada ano estão no período.
	"""
	if df_filtered.empty or df_limits.empty:
		return pd.DataFrame()

	# Gasto real por secretaria (de todo o período filtrado)
	real = (
		df_filtered.groupby("secretaria", as_index=False)
		.agg(gasto_valor=("valor", "sum"), gasto_litros=("litros", "sum"))
	)

	# Contar meses por ano no período
	df_filtered_copy = df_filtered.copy()
	if "data_hora" in df_filtered_copy.columns and not df_filtered_copy.empty:
		df_filtered_copy["ano"] = pd.to_datetime(df_filtered_copy["data_hora"]).dt.year
		df_filtered_copy["ano_mes"] = pd.to_datetime(df_filtered_copy["data_hora"]).dt.to_period("M")
		months_by_year = df_filtered_copy.groupby("ano")["ano_mes"].nunique().to_dict()
	else:
		# Se não há data, assume 1 mês
		months_by_year = {}

	# Agregar limites por secretaria, somando do limite_mensal * meses de cada ano
	base = df_limits.copy()
	if "limite_mensal" not in base.columns:
		base["limite_mensal"] = 0.0
	if "limite_litros_mensal" not in base.columns:
		base["limite_litros_mensal"] = 0.0
	if "ano" not in base.columns:
		# Se df_limits não tem coluna ano, assume que é do ano corrente
		base["ano"] = datetime.date.today().year

	# Calcular limite_valor_periodo por secretaria somando os anos
	aggregated_limits = []
	for sec in base["secretaria"].unique():
		sec_data = base[base["secretaria"] == sec]
		limite_valor_total = 0
		limite_litros_total = 0
		valor_empenhado_total = 0
		limite_mensal_avg = 0.0  # Média do limite mensal para referência
		limite_litros_mensal_avg = 0.0

		for _, row in sec_data.iterrows():
			ano = int(row["ano"])
			meses_neste_ano = months_by_year.get(ano, 0)
			if meses_neste_ano > 0:
				limite_valor_total += float(row["limite_mensal"]) * meses_neste_ano
				limite_litros_total += float(row["limite_litros_mensal"]) * meses_neste_ano
			valor_empenhado_total += float(row.get("valor_empenhado", 0.0))
			limite_mensal_avg += float(row["limite_mensal"])
			limite_litros_mensal_avg += float(row["limite_litros_mensal"])

		# Calcular média dos limites mensais disponíveis
		n_years = len(sec_data)
		limite_mensal_avg = limite_mensal_avg / n_years if n_years > 0 else 0.0
		limite_litros_mensal_avg = limite_litros_mensal_avg / n_years if n_years > 0 else 0.0

		aggregated_limits.append({
			"secretaria": sec,
			"limite_valor_periodo": limite_valor_total,
			"limite_litros_periodo": limite_litros_total,
			"valor_empenhado_total": valor_empenhado_total,
			"limite_mensal": limite_mensal_avg,
			"limite_litros_mensal": limite_litros_mensal_avg,
		})

	base_agg = pd.DataFrame(aggregated_limits)

	# Merge com dados reais
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
		else ("ESTOURO GERAL" if r["estourou_valor"] and r["estourou_litros"] else "OK"),
		axis=1,
	)
	return merged


def build_kpis(
    df_filtered: pd.DataFrame,
    status_df: pd.DataFrame,
    df_limits: pd.DataFrame,
    usar_limite_quinzenal_secretaria: bool = False,
) -> dict:
    gasto_total = float(df_filtered["valor"].sum())
    gasto_bruto_total = float(df_filtered["valor_bruto"].sum()) if "valor_bruto" in df_filtered.columns else gasto_total
    desconto_total = float(df_filtered["desconto_valor"].sum()) if "desconto_valor" in df_filtered.columns else 0.0
    gasto_litros = float(df_filtered["litros"].sum())
    km_total = float(df_filtered["km_rodado"].sum()) if "km_rodado" in df_filtered.columns else 0.0
    consumo_medio = float(df_filtered["km_por_litro"].mean()) if "km_por_litro" in df_filtered.columns and df_filtered["km_por_litro"].notna().any() else 0.0
    custo_por_km = float(df_filtered["custo_por_km"].mean()) if "custo_por_km" in df_filtered.columns and df_filtered["custo_por_km"].notna().any() else 0.0
    n_abastecimentos = len(df_filtered)
    veiculos_ativos = df_filtered["placa"].nunique() if "placa" in df_filtered.columns else 0

    limite_total_periodo = float(status_df["limite_valor_periodo"].sum()) if "limite_valor_periodo" in status_df.columns else 0.0

    months = month_count(df_filtered)
    # Valor empenhado é a soma do empenho total (anual) dos anos no período
    valor_empenhado = float(df_limits["valor_empenhado"].sum())
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
        # operacionais
        "km_total": km_total,
        "consumo_medio": consumo_medio,
        "custo_por_km": custo_por_km,
        "n_abastecimentos": n_abastecimentos,
        "veiculos_ativos": veiculos_ativos,
    }


def make_bar_consumo_tipo_mes(df_filtered: pd.DataFrame) -> go.Figure:
    if df_filtered.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados para consumo por tipo e mês")
        return apply_plotly_theme(fig)

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

    def corrige_mes_nome(mes_nome):
        return "Março" if str(mes_nome).strip().lower() == "marco" else mes_nome

    grupo["periodo"] = grupo.apply(lambda row: f"{corrige_mes_nome(row['mes_nome'])}/{int(row['ano'])}", axis=1)

    color_map = {"GASOLINA": "#2563eb", "DIESEL": "#f97316", "ALCOOL": "#22c55e"}
    fig = go.Figure()
    for fuel in ["GASOLINA", "DIESEL", "ALCOOL"]:
        dados = grupo[grupo["combustivel_grupo"] == fuel]
        if dados.empty:
            continue
        fig.add_trace(
            go.Bar(
                x=dados["periodo"],
                y=dados["valor_total"],
                name=fuel.title(),
                marker_color=color_map[fuel],
                text=[currency(v) for v in dados["valor_total"]],
                textposition="outside",
                offsetgroup=fuel,
                legendgroup=fuel,
                showlegend=True,
                textfont={"size": 13, "color": "#fff", "family": "'Space Grotesk', sans-serif"},
            )
        )
    fig.update_layout(
        template="plotly_dark",
        xaxis_title={"text": "Período", "font": {"size": 16}},
        yaxis_title={"text": "Valor faturado (R$)", "font": {"size": 16}},
        xaxis={"tickfont": {"size": 15}},
        yaxis={"tickfont": {"size": 14}},
        barmode="group",
        bargap=0.18,
        bargroupgap=0.08,
        margin={"l": 30, "r": 30, "t": 90, "b": 30},
        legend={
            "orientation": "h",
            "x": 0.01,
            "y": 1.04,
            "xanchor": "left",
            "yanchor": "bottom",
            "font": {"size": 16, "color": "#eaf2ff"},
        },
    )
    fig = apply_plotly_theme(fig)
    fig.update_layout(
        title="Consumo de combustível por mês e tipo (R$)",
        legend={"font": {"size": 13, "color": "#eaf2ff"}, "orientation": "h", "x": 0.5, "y": -0.18, "xanchor": "center", "yanchor": "top"},
        xaxis={"tickfont": {"size": 13}},
        yaxis={"tickfont": {"size": 13}},
        margin={"l": 20, "r": 20, "t": 48, "b": 60},
    )
    return fig


def make_ranking_consumo_secretaria(status_df: pd.DataFrame) -> go.Figure:
    """Ranking de consumo em R$ por secretaria com saldo do empenho empilhado em cinza."""
    if status_df.empty or "gasto_valor" not in status_df.columns:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados")
        return apply_plotly_theme(fig)

    df = status_df[status_df["gasto_valor"] > 0].copy()
    # Usar valor_empenhado_total (novo campo agregado) em vez de empenho_2026
    df["saldo_empenho"] = (df["valor_empenhado_total"] - df["gasto_valor"]).clip(lower=0) if "valor_empenhado_total" in df.columns else 0
    df = df.sort_values("gasto_valor", ascending=True)

    def moeda_br(v):
        return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    max_val = (df["valor_empenhado_total"] if "valor_empenhado_total" in df.columns else df["gasto_valor"]).max()

    fig = go.Figure()
    # Barra azul: gasto
    fig.add_trace(go.Bar(
        y=df["secretaria"],
        x=df["gasto_valor"],
        orientation="h",
        name="Gasto",
        marker_color="#2563eb",
        text=[moeda_br(v) for v in df["gasto_valor"]],
        textposition="inside",
        insidetextanchor="start",
        textfont={"size": 12, "color": "#fff"},
        hovertemplate="<b>%{y}</b><br>Gasto: R$ %{x:,.2f}<extra></extra>",
    ))
    # Barra cinza: saldo do empenho
    if "valor_empenhado_total" in df.columns:
        fig.add_trace(go.Bar(
            y=df["secretaria"],
            x=df["saldo_empenho"],
            orientation="h",
            name="Saldo do empenho",
            marker_color="#475569",
            marker_opacity=0.5,
            text=[moeda_br(v) if v > 0 else "" for v in df["saldo_empenho"]],
            textposition="inside",
            insidetextanchor="end",
            textfont={"size": 11, "color": "#cbd5e1"},
            hovertemplate="<b>%{y}</b><br>Saldo: R$ %{x:,.2f}<extra></extra>",
        ))
    fig.update_layout(
        barmode="stack",
        template="plotly_dark",
        title="Ranking de Consumo por Secretaria (R$)",
        xaxis_title="R$",
        xaxis={"range": [0, max_val * 1.02]},
        margin={"l": 20, "r": 20, "t": 48, "b": 60},
        height=max(480, 42 * len(df)),
        bargap=0.25,
        legend={
            "orientation": "h", "x": 0.5, "y": -0.08,
            "xanchor": "center", "yanchor": "top",
            "font": {"size": 13, "color": "#eaf2ff"},
            "bgcolor": "rgba(0,0,0,0)",
        },
    )
    fig = apply_plotly_theme(fig)
    return fig


def make_bar_valor_vs_limite_secretaria(status_df: pd.DataFrame) -> go.Figure:
    """Gráfico de barras horizontais: % do limite de R$ consumido por secretaria."""
    if status_df.empty or "gasto_valor" not in status_df.columns:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados")
        return apply_plotly_theme(fig)

    df = status_df[status_df["limite_valor_periodo"] > 0].copy()
    if df.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem limites de valor definidos")
        return apply_plotly_theme(fig)

    df["pct"] = df["gasto_valor"] / df["limite_valor_periodo"] * 100
    df["desvio_valor"] = df["gasto_valor"] - df["limite_valor_periodo"]
    df = df.sort_values("pct", ascending=True)

    # Deduz número de meses a partir de limite_valor_periodo / limite_mensal
    _ref = df[df["limite_mensal"] > 0]
    if not _ref.empty:
        _months = round((_ref["limite_valor_periodo"] / _ref["limite_mensal"]).mean())
        months_label = f"1 mês" if _months <= 1 else f"{int(_months)} meses"
    else:
        months_label = "período"

    def bar_color(pct):
        if pct > 100:
            return "#ef4444"
        if pct > 80:
            return "#eab308"
        return "#22c55e"

    colors = [bar_color(p) for p in df["pct"]]
    pct_clip = df["pct"].clip(upper=200)

    # Label curto: só o % (com ⚠ se excedido). Detalhes ficam no hover.
    def bar_label(p):
        if p > 100:
            return f"⚠ {p:.0f}%"
        return f"{p:.0f}%"

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df["secretaria"],
        x=pct_clip,
        orientation="h",
        marker_color=colors,
        text=[bar_label(p) for p in df["pct"]],
        textposition="outside",
        textfont={"size": 12, "color": "#eaf2ff"},
        customdata=list(zip(df["gasto_valor"], df["limite_valor_periodo"], df["pct"], df["desvio_valor"])),
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Gasto: R$ %{customdata[0]:,.2f}<br>"
            "Limite: R$ %{customdata[1]:,.2f}<br>"
            f"  (limite mensal × {months_label})<br>"
            "Uso: %{customdata[2]:.1f}%<br>"
            "Saldo / Excesso: R$ %{customdata[3]:,.2f}<extra></extra>"
        ),
        showlegend=False,
    ))
    fig.add_vline(x=100, line_dash="dash", line_color="#eab308", line_width=2)

    max_pct = max(df["pct"].max(), 100)
    x_range_max = max_pct * 1.18  # margem só para o label curto caber

    fig.update_layout(
        template="plotly_dark",
        xaxis_title="% do limite consumido",
        xaxis={"range": [0, x_range_max], "ticksuffix": "%"},
        title="Gasto em R$ vs. limite por secretaria",
        margin={"l": 20, "r": 20, "t": 48, "b": 30},
        height=max(400, 30 * len(df)),
    )
    fig = apply_plotly_theme(fig)
    return fig


def make_bar_litros_vs_limite_secretaria(df_filtered: pd.DataFrame, df_limits: pd.DataFrame) -> go.Figure:
    """Gráfico de barras horizontais: % do limite de litros consumido por secretaria e combustível."""
    if df_filtered.empty or df_limits.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados")
        return apply_plotly_theme(fig)

    months = month_count(df_filtered)

    df = df_filtered.copy()
    df["combustivel_grupo"] = df["combustivel"].map(
        lambda v: "DIESEL" if str(v).upper().startswith("DIESEL")
        else ("ALCOOL" if str(v).upper() in {"ALCOOL", "ETANOL"} else str(v).upper())
    )
    df = df[df["combustivel_grupo"].isin(["GASOLINA", "DIESEL", "ALCOOL"])]
    consumed = df.groupby(["secretaria", "combustivel_grupo"], as_index=False).agg(litros=("litros", "sum"))

    fuel_limit_col = {"GASOLINA": "limite_litros_gasolina", "ALCOOL": "limite_litros_alcool", "DIESEL": "limite_litros_diesel"}
    rows = []
    for _, row in df_limits.iterrows():
        for fuel, col in fuel_limit_col.items():
            lim = float(row.get(col, 0)) * months
            if lim > 0:
                rows.append({"secretaria": row["secretaria"], "combustivel_grupo": fuel, "limite": lim})
    if not rows:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem limites por litro definidos")
        return apply_plotly_theme(fig)

    limits_df = pd.DataFrame(rows)
    merged = limits_df.merge(consumed, on=["secretaria", "combustivel_grupo"], how="left").fillna(0)
    merged["pct"] = merged.apply(lambda r: r["litros"] / r["limite"] * 100 if r["limite"] > 0 else 0, axis=1)
    merged["label_y"] = merged["secretaria"] + " · " + merged["combustivel_grupo"].str.title()
    merged = merged.sort_values(["secretaria", "combustivel_grupo"])

    def bar_color(pct):
        if pct > 100:
            return "#ef4444"
        if pct > 80:
            return "#eab308"
        return "#22c55e"

    consumed_pct = merged["pct"].clip(upper=150)
    colors = [bar_color(p) for p in merged["pct"]]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=merged["label_y"],
        x=consumed_pct,
        orientation="h",
        marker_color=colors,
        text=[
            f"{p:.0f}% ({v:,.0f} L / {l:,.0f} L)".replace(",", ".")
            for p, v, l in zip(merged["pct"], merged["litros"], merged["limite"])
        ],
        textposition="outside",
        textfont={"size": 12, "color": "#eaf2ff"},
        customdata=list(zip(merged["litros"], merged["limite"], merged["pct"])),
        hovertemplate="<b>%{y}</b><br>Consumido: %{customdata[0]:,.0f} L<br>Limite: %{customdata[1]:,.0f} L<br>Uso: %{customdata[2]:.1f}%<extra></extra>",
        showlegend=False,
    ))
    # Linha de referência 100%
    fig.add_vline(x=100, line_dash="dash", line_color="#eab308", line_width=2)
    fig.update_layout(
        template="plotly_dark",
        xaxis_title="% do limite consumido",
        xaxis={"range": [0, 155], "ticksuffix": "%"},
        title="Consumo de litros vs. limite por secretaria",
        margin={"l": 20, "r": 60, "t": 48, "b": 30},
        height=max(400, 28 * len(merged)),
    )
    fig = apply_plotly_theme(fig)
    return fig


def make_bar_consumo_tipo_mes_litros(df_filtered: pd.DataFrame) -> go.Figure:
    if df_filtered.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados para consumo por tipo e mês")
        return apply_plotly_theme(fig)

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
        .agg(litros_total=("litros", "sum"))
        .sort_values(["ano", "mes", "combustivel_grupo"])
    )

    def corrige_mes_nome(mes_nome):
        return "Março" if str(mes_nome).strip().lower() == "marco" else mes_nome

    grupo["periodo"] = grupo.apply(lambda row: f"{corrige_mes_nome(row['mes_nome'])}/{int(row['ano'])}", axis=1)

    color_map = {"GASOLINA": "#2563eb", "DIESEL": "#f97316", "ALCOOL": "#22c55e"}
    fig = go.Figure()
    for fuel in ["GASOLINA", "DIESEL", "ALCOOL"]:
        dados = grupo[grupo["combustivel_grupo"] == fuel]
        if dados.empty:
            continue
        fig.add_trace(
            go.Bar(
                x=dados["periodo"],
                y=dados["litros_total"],
                name=fuel.title(),
                marker_color=color_map[fuel],
                text=[f"{v:,.0f} L" for v in dados["litros_total"]],
                textposition="outside",
                offsetgroup=fuel,
                legendgroup=fuel,
                showlegend=True,
                textfont={"size": 16, "color": "#fff", "family": "'Space Grotesk', sans-serif"},
            )
        )
    fig.update_layout(
        template="plotly_dark",
        xaxis_title={"text": "Período", "font": {"size": 14}},
        yaxis_title={"text": "Volume (Litros)", "font": {"size": 14}},
        barmode="group",
        bargap=0.18,
        bargroupgap=0.08,
        margin={"l": 30, "r": 30, "t": 90, "b": 30},
        legend={
            "orientation": "h",
            "x": 0.01,
            "y": 1.04,
            "xanchor": "left",
            "yanchor": "bottom",
            "font": {"size": 12, "color": "#eaf2ff"},
        },
    )
    fig = apply_plotly_theme(fig)
    fig.update_layout(title="Consumo de combustível por mês e tipo (Litros)", margin={"l": 20, "r": 20, "t": 48, "b": 60},
        legend={"font": {"size": 13, "color": "#eaf2ff"}, "orientation": "h", "x": 0.5, "y": -0.18, "xanchor": "center", "yanchor": "top"})
    return fig


def make_line_custo_medio_mes_combustivel(df_filtered: pd.DataFrame) -> go.Figure:
    if df_filtered.empty or not {"mes", "ano", "combustivel", "valor", "litros"}.issubset(df_filtered.columns):
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Custo médio de combustível por mês")
        return apply_plotly_theme(fig)

    df = df_filtered.copy()
    grupo = df.groupby(["ano", "mes", "combustivel"], as_index=False).agg(
        litros=("litros", "sum"),
    )
    grupo["mes_label"] = grupo["mes"].apply(lambda m: MONTHS[m])

    color_map = {
        "GASOLINA": "#2563eb",
        "DIESEL": "#f97316",
        "DIESEL S10": "#f97316",
        "ALCOOL": "#22c55e",
        "ETANOL": "#22c55e",
    }
    fig = go.Figure()
    for combustivel in grupo["combustivel"].unique():
        dados = grupo[grupo["combustivel"] == combustivel]
        fig.add_trace(go.Scatter(
            x=dados["mes_label"],
            y=dados["litros"],
            mode="lines+markers",
            name=str(combustivel),
            text=[f"{v:,.0f} L" for v in dados["litros"]],
            textposition="top center",
            line={"color": color_map.get(str(combustivel).upper(), "#38bdf8"), "width": 3},
            marker={"color": color_map.get(str(combustivel).upper(), "#38bdf8")},
        ))
    fig.update_layout(
        template="plotly_dark",
        xaxis_title="Mês",
        yaxis_title="Volume (Litros)",
        title="Consumo por combustível por mês (Litros)",
        margin={"l": 30, "r": 30, "t": 48, "b": 30},
        legend={"font": {"size": 13, "color": "#eaf2ff"}},
    )
    fig = apply_plotly_theme(fig)
    return fig


def make_ranking_veiculos(df_filtered: pd.DataFrame, top_n: int = 20) -> go.Figure:
    """Top N veículos por gasto total (R$), identificados por Marca Modelo — PLACA | Secretaria."""
    needed = {"placa", "marca", "modelo", "valor", "litros"}
    if df_filtered.empty or not needed.issubset(df_filtered.columns):
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Ranking de veículos — sem dados")
        return apply_plotly_theme(fig)

    df = df_filtered.copy()
    has_sec = "secretaria" in df.columns
    group_cols = ["marca", "modelo", "placa"] + (["secretaria"] if has_sec else [])

    grp = (
        df.groupby(group_cols, as_index=False)
        .agg(valor=("valor", "sum"), litros=("litros", "sum"), abastecimentos=("placa", "count"))
        .sort_values("valor", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    grp["rank"] = range(1, len(grp) + 1)
    grp = grp.sort_values("valor", ascending=True).reset_index(drop=True)

    def _label(row):
        veiculo = (
            row["marca"].strip().title()
            + " "
            + row["modelo"].strip().title()
            + " — "
            + row["placa"].strip().upper()
        )
        if has_sec and row.get("secretaria", ""):
            veiculo += f"  |  {str(row['secretaria']).strip()}"
        return f"#{int(row['rank'])}  {veiculo}"

    labels_y = [_label(row) for _, row in grp.iterrows()]
    texto = [f"R$ {v:,.0f}".replace(",", ".") for v in grp["valor"]]

    fig = go.Figure(go.Bar(
        y=labels_y,
        x=grp["valor"],
        orientation="h",
        marker={"color": "#2563eb", "line": {"width": 0}},
        text=texto,
        textposition="outside",
        textfont={"size": 12, "color": "#eaf2ff", "family": "Rajdhani, sans-serif"},
        customdata=list(zip(grp["litros"], grp["abastecimentos"])),
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Gasto: R$ %{x:,.2f}<br>"
            "Litros: %{customdata[0]:,.1f} L<br>"
            "Abastecimentos: %{customdata[1]}<extra></extra>"
        ),
        showlegend=False,
    ))

    max_val = grp["valor"].max() if not grp.empty else 1
    fig.update_layout(
        template="plotly_dark",
        title={"text": f"🏆 Top {top_n} veículos por gasto", "font": {"size": 18, "family": "Rajdhani, sans-serif"}},
        xaxis={
            "title": "Gasto total (R$)",
            "tickprefix": "R$ ",
            "showgrid": True,
            "gridcolor": "rgba(255,255,255,0.07)",
            "range": [0, max_val * 1.22],
        },
        yaxis={
            "tickfont": {"size": 12, "family": "Rajdhani, sans-serif"},
            "showgrid": False,
        },
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin={"l": 10, "r": 20, "t": 50, "b": 30},
        height=max(480, 32 * top_n),
        bargap=0.35,
    )
    return apply_plotly_theme(fig)


def make_treemap_postos(df_filtered: pd.DataFrame) -> go.Figure:
    """Treemap dos postos de abastecimento por valor total gasto."""
    if df_filtered.empty or "posto" not in df_filtered.columns:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Treemap de postos — sem dados")
        return apply_plotly_theme(fig)

    df = df_filtered.copy()
    df["posto"] = df["posto"].fillna("Desconhecido").str.strip()
    df = df[df["posto"] != ""]

    grp = (
        df.groupby("posto", as_index=False)
        .agg(valor=("valor", "sum"), litros=("litros", "sum"), abastecimentos=("placa", "count"))
        .sort_values("valor", ascending=False)
    )

    fig = go.Figure(go.Treemap(
        labels=grp["posto"],
        parents=[""] * len(grp),
        values=grp["valor"],
        customdata=list(zip(grp["litros"], grp["abastecimentos"])),
        texttemplate="<b>%{label}</b><br>R$ %{value:,.0f}",
        hovertemplate=(
            "<b>%{label}</b><br>"
            "Gasto: R$ %{value:,.2f}<br>"
            "Litros: %{customdata[0]:,.1f} L<br>"
            "Abastecimentos: %{customdata[1]}<extra></extra>"
        ),
        marker={
            "colorscale": [
                [0.0, "#1e3a5f"],
                [0.5, "#1d6fa4"],
                [1.0, "#38bdf8"],
            ],
            "colors": grp["valor"].tolist(),
            "showscale": False,
        },
        textfont={"size": 13, "color": "#eaf2ff"},
    ))
    fig.update_layout(
        template="plotly_dark",
        title="Postos de abastecimento — gasto total (R$)",
        margin={"l": 10, "r": 10, "t": 48, "b": 10},
        height=520,
    )
    return apply_plotly_theme(fig)


def make_line_custo_medio_rl_combustivel(df_filtered: pd.DataFrame) -> go.Figure:
    """Custo médio (R$/L) por tipo de combustível ao longo dos meses."""
    if df_filtered.empty or not {"mes", "combustivel", "valor", "litros"}.issubset(df_filtered.columns):
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Custo médio R$/L por combustível")
        return apply_plotly_theme(fig)

    df = df_filtered.copy()
    # Unificar variantes do mesmo combustível antes de agrupar
    df["combustivel"] = df["combustivel"].replace({"DIESEL S10": "DIESEL", "ETANOL": "ALCOOL"})
    grupo = (
        df.groupby(["mes", "combustivel"], as_index=False)
        .agg(valor=("valor", "sum"), litros=("litros", "sum"))
    )
    grupo = grupo[grupo["litros"] > 0].copy()
    grupo["custo_medio"] = grupo["valor"] / grupo["litros"]
    grupo["mes_label"] = grupo["mes"].map(MONTHS)
    grupo = grupo.sort_values("mes")

    color_map = {
        "GASOLINA": "#2563eb",
        "DIESEL": "#f97316",
        "DIESEL S10": "#f97316",
        "ALCOOL": "#22c55e",
        "ETANOL": "#22c55e",
    }

    fig = go.Figure()
    for combustivel in sorted(grupo["combustivel"].unique()):
        dados = grupo[grupo["combustivel"] == combustivel].sort_values("mes")
        cor = color_map.get(str(combustivel).upper(), "#94a3b8")
        fig.add_trace(go.Scatter(
            x=dados["mes_label"],
            y=dados["custo_medio"],
            mode="lines+markers",
            name=str(combustivel),
            line={"color": cor, "width": 3},
            marker={"color": cor, "size": 7},
            hovertemplate=(
                "<b>%{x}</b><br>"
                f"{combustivel}<br>"
                "Custo médio: R$ %{y:.3f}/L<extra></extra>"
            ),
        ))
    fig.update_layout(
        template="plotly_dark",
        title="Custo médio R$/L por combustível",
        xaxis={
            "title": "Mês",
            "categoryorder": "array",
            "categoryarray": [MONTHS[m] for m in sorted(MONTHS)],
        },
        yaxis_title="R$/L",
        yaxis_tickprefix="R$ ",
        yaxis_tickformat=".2f",
        margin={"l": 30, "r": 20, "t": 48, "b": 60},
        legend={
            "orientation": "h",
            "x": 0.5,
            "y": -0.18,
            "xanchor": "center",
            "yanchor": "top",
            "font": {"size": 13, "color": "#eaf2ff"},
            "bgcolor": "rgba(8, 17, 28, 0.75)",
        },
    )
    return apply_plotly_theme(fig)


def make_bar_comparativo_mensal_yoy(df_scope: pd.DataFrame, data_inicio, data_fim) -> go.Figure:
	"""Barras agrupadas comparando o mesmo mês entre anos, filtrado pelo período selecionado."""
	required = {"ano", "mes", "valor"}
	if df_scope.empty or not required.issubset(df_scope.columns):
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Comparativo Mensal por Ano")
		return apply_plotly_theme(fig)

	months_short = {
		1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
		7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
	}

	# Determina meses a exibir com base no período selecionado
	if data_inicio and data_fim:
		_meses_ini = data_inicio.month
		_meses_fim = data_fim.month
		if data_inicio.year == data_fim.year:
			meses_filtro = list(range(_meses_ini, _meses_fim + 1))
		else:
			meses_filtro = list(range(1, 13))
	else:
		meses_filtro = list(range(1, 13))

	df = df_scope.copy()
	df = df[df["mes"].isin(meses_filtro)]
	grupo = (
		df.groupby(["ano", "mes"], as_index=False)
		.agg(valor_total=("valor", "sum"))
	)
	if grupo.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Comparativo Mensal por Ano")
		return apply_plotly_theme(fig)

	PALETA = {
		2024: "#7dd3fc",
		2025: "#60a5fa",
		2026: "#1d4ed8",
	}
	anos = sorted(grupo["ano"].unique())
	meses_presentes = sorted(grupo["mes"].unique())
	meses_labels = [months_short[m] for m in meses_presentes]

	fig = go.Figure()
	for ano in anos:
		dados = grupo[grupo["ano"] == ano].set_index("mes")
		y_vals = [float(dados.loc[m, "valor_total"]) if m in dados.index else None for m in meses_presentes]
		fig.add_trace(go.Bar(
			name=str(int(ano)),
			x=meses_labels,
			y=y_vals,
			marker_color=PALETA.get(int(ano), "#94a3b8"),
			text=[currency(v) if v else "" for v in y_vals],
			textposition="outside",
			textfont={"size": 11, "color": "#e7eef8"},
			hovertemplate="<b>%{x} " + str(int(ano)) + "</b><br>R$ %{y:,.2f}<extra></extra>",
		))

	_title = "Comparativo Mensal por Ano"
	if data_inicio and data_fim and data_inicio.year == data_fim.year:
		_title = f"Comparativo Mensal: {data_inicio.year} vs anos anteriores"

	fig.update_layout(
		template="plotly_dark",
		title=_title,
		barmode="group",
		bargap=0.18,
		bargroupgap=0.05,
		xaxis_title="Mês",
		yaxis_title="Valor (R$)",
		yaxis_tickprefix="R$ ",
		yaxis_tickformat=",.0f",
		margin={"l": 30, "r": 20, "t": 54, "b": 60},
		legend={"orientation": "h", "x": 0.02, "y": 0.99, "xanchor": "left", "yanchor": "top",
				"font": {"size": 13, "color": "#eaf2ff"}, "bgcolor": "rgba(8,17,28,0.65)"},
	)
	return apply_plotly_theme(fig)


def make_bar_comparativo_mensal_yoy_litros(df_scope: pd.DataFrame, data_inicio, data_fim) -> go.Figure:
	"""Barras agrupadas comparando litros consumidos no mesmo mês entre anos."""
	required = {"ano", "mes", "litros"}
	if df_scope.empty or not required.issubset(df_scope.columns):
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Comparativo Mensal por Ano (Litros)")
		return apply_plotly_theme(fig)

	months_short = {
		1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
		7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
	}

	if data_inicio and data_fim:
		if data_inicio.year == data_fim.year:
			meses_filtro = list(range(data_inicio.month, data_fim.month + 1))
		else:
			meses_filtro = list(range(1, 13))
	else:
		meses_filtro = list(range(1, 13))

	df = df_scope[df_scope["mes"].isin(meses_filtro)].copy()
	grupo = df.groupby(["ano", "mes"], as_index=False).agg(litros_total=("litros", "sum"))
	if grupo.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Comparativo Mensal por Ano (Litros)")
		return apply_plotly_theme(fig)

	PALETA = {2024: "#7dd3fc", 2025: "#60a5fa", 2026: "#1d4ed8"}
	anos = sorted(grupo["ano"].unique())
	meses_presentes = sorted(grupo["mes"].unique())
	meses_labels = [months_short[m] for m in meses_presentes]

	fig = go.Figure()
	for ano in anos:
		dados = grupo[grupo["ano"] == ano].set_index("mes")
		y_vals = [float(dados.loc[m, "litros_total"]) if m in dados.index else None for m in meses_presentes]
		fmt_vals = [f"{v:,.0f} L".replace(",", "X").replace(".", ",").replace("X", ".") if v else "" for v in y_vals]
		fig.add_trace(go.Bar(
			name=str(int(ano)),
			x=meses_labels,
			y=y_vals,
			marker_color=PALETA.get(int(ano), "#94a3b8"),
			text=fmt_vals,
			textposition="outside",
			textfont={"size": 11, "color": "#e7eef8"},
			hovertemplate="<b>%{x} " + str(int(ano)) + "</b><br>%{y:,.0f} L<extra></extra>",
		))

	fig.update_layout(
		template="plotly_dark",
		title="Comparativo Mensal por Ano (Litros)",
		barmode="group",
		bargap=0.18,
		bargroupgap=0.05,
		xaxis_title="Mês",
		yaxis_title="Litros",
		yaxis_tickformat=",.0f",
		margin={"l": 30, "r": 20, "t": 54, "b": 60},
		legend={"orientation": "h", "x": 0.02, "y": 0.99, "xanchor": "left", "yanchor": "top",
				"font": {"size": 13, "color": "#eaf2ff"}, "bgcolor": "rgba(8,17,28,0.65)"},
	)
	return apply_plotly_theme(fig)


def make_line_sazonalidade_yoy(df_scope: pd.DataFrame) -> go.Figure:
	"""Overlay mensal por ano para comparação de sazonalidade (YoY)."""
	required = {"ano", "mes", "valor"}
	if df_scope.empty or not required.issubset(df_scope.columns):
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Comparativo de Sazonalidade YoY")
		return apply_plotly_theme(fig)

	df = df_scope.copy()
	grupo = (
		df.groupby(["ano", "mes"], as_index=False)
		.agg(valor_total=("valor", "sum"))
		.sort_values(["ano", "mes"])
	)
	if grupo.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Comparativo de Sazonalidade YoY")
		return apply_plotly_theme(fig)

	months_short = {
		1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
		7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
	}
	month_order = [months_short[m] for m in range(1, 13)]
	anos = sorted(int(a) for a in grupo["ano"].dropna().unique())
	ano_atual = max(anos)

	# Anos anteriores em tons de azul mais claros, porém com contraste entre si.
	styles_antigos = [
		{"color": "#bfdbfe", "dash": "dot", "width": 2},
		{"color": "#38bdf8", "dash": "dash", "width": 3},
		{"color": "#60a5fa", "dash": "dashdot", "width": 2},
		{"color": "#93c5fd", "dash": "dot", "width": 2},
	]

	fig = go.Figure()
	antigos = [a for a in anos if a != ano_atual]
	for idx, ano in enumerate(antigos):
		dados = grupo[grupo["ano"] == ano].copy()
		dados["mes_label"] = dados["mes"].map(months_short)
		estilo = styles_antigos[min(idx, len(styles_antigos) - 1)]
		fig.add_trace(go.Scatter(
			x=dados["mes_label"],
			y=dados["valor_total"],
			mode="lines",
			name=str(ano),
			line={"color": estilo["color"], "width": estilo["width"], "dash": estilo["dash"]},
			hovertemplate="<b>%{x}</b><br>Ano: " + str(ano) + "<br>Valor: R$ %{y:,.2f}<extra></extra>",
		))

	dados_atual = grupo[grupo["ano"] == ano_atual].copy()
	dados_atual["mes_label"] = dados_atual["mes"].map(months_short)
	fig.add_trace(go.Scatter(
		x=dados_atual["mes_label"],
		y=dados_atual["valor_total"],
		mode="lines+markers",
		name=f"{ano_atual} (Atual)",
		line={"color": "#1d4ed8", "width": 4},
		marker={"color": "#1d4ed8", "size": 8},
		hovertemplate="<b>%{x}</b><br>Ano: " + str(ano_atual) + " (Atual)<br>Valor: R$ %{y:,.2f}<extra></extra>",
	))

	fig.update_layout(
		template="plotly_dark",
		title="Comparativo de Sazonalidade YoY",
		xaxis={
			"title": "Mês",
			"categoryorder": "array",
			"categoryarray": month_order,
		},
		yaxis_title="Valor total (R$)",
		yaxis_tickprefix="R$ ",
		yaxis_tickformat=",.0f",
		margin={"l": 30, "r": 20, "t": 54, "b": 60},
		legend={
			"orientation": "h",
			"x": 0.02,
			"y": 0.99,
			"xanchor": "left",
			"yanchor": "top",
			"font": {"size": 13, "color": "#eaf2ff"},
			"bgcolor": "rgba(8, 17, 28, 0.65)",
		},
	)
	return apply_plotly_theme(fig)


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

    mensal_real = (
        df_filtered.groupby(["ano", "mes"], as_index=False)
        .agg(valor=("valor", "sum"))
        .sort_values(["ano", "mes"])
    )
    mensal_real["acumulado_real"] = mensal_real["valor"].cumsum()
    # Usar nome do mês via MONTHS para garantir consistência com a linha prevista
    mensal_real["mes_label"] = mensal_real["mes"].map(MONTHS)

    if usar_limite_quinzenal_secretaria and not df_limits.empty and "limite_mensal" in df_limits.columns:
        previsto_mensal = float(df_limits["limite_mensal"].sum())
        previsto_total = previsto_mensal * 12
    else:
        empenho_total = float(df_limits["valor_empenhado"].sum())
        previsto_total = empenho_total
        previsto_mensal = previsto_total / 12.0

    meses_previstos = [MONTHS[m] for m in range(1, 13)]
    acumulado_previsto = [previsto_mensal * (i + 1) for i in range(12)]
    previsto_map = {mes: val for mes, val in zip(meses_previstos, acumulado_previsto)}

    xs = mensal_real["mes_label"].tolist()
    ys = mensal_real["acumulado_real"].tolist()
    acima = [y > previsto_map.get(x, float("inf")) for x, y in zip(xs, ys)]
    marker_colors = ["#ef4444" if a else "#22c55e" for a in acima]

    fig = go.Figure()

    # Desenha segmentos: cada par de pontos consecutivos em verde ou vermelho
    # Agrupa segmentos contíguos da mesma cor para minimizar traces
    i = 0
    legend_green_added = False
    legend_red_added = False
    while i < len(xs):
        cor = "#ef4444" if acima[i] else "#22c55e"
        nome = "Acima do previsto" if acima[i] else "Real acumulado"
        show_legend = (acima[i] and not legend_red_added) or (not acima[i] and not legend_green_added)
        # Coleta segmento contíguo da mesma cor
        seg_x = [xs[i]]
        seg_y = [ys[i]]
        while i + 1 < len(xs) and acima[i + 1] == acima[i]:
            i += 1
            seg_x.append(xs[i])
            seg_y.append(ys[i])
        # Adiciona o primeiro ponto do próximo segmento para não haver lacuna na linha
        if i + 1 < len(xs):
            seg_x.append(xs[i + 1])
            seg_y.append(ys[i + 1])
        fig.add_trace(
            go.Scatter(
                x=seg_x,
                y=seg_y,
                mode="lines",
                name=nome,
                line={"color": cor, "width": 3},
                showlegend=show_legend,
                legendgroup=nome,
                hoverinfo="skip",
            )
        )
        if acima[i]:
            legend_red_added = True
        else:
            legend_green_added = True
        i += 1

    # Marcadores e labels por cima (trace separado, sem linha)
    fig.add_trace(
        go.Scatter(
            x=xs,
            y=ys,
            mode="markers+text",
            name="",
            showlegend=False,
            marker={"color": marker_colors, "size": 10, "line": {"color": "#fff", "width": 1}},
            text=[f"{v:,.0f}".replace(",", ".") for v in ys],
            textposition="bottom center",
            textfont={"size": 14},
            hovertemplate="<b>%{x}</b><br>Real acumulado: R$ %{y:,.2f}<extra></extra>",
        )
    )

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
        title="Gasto acumulado: Real x Previsto",
        margin={"l": 30, "r": 30, "t": 48, "b": 60},
        legend={
            "orientation": "h",
            "x": 0.5,
            "y": -0.18,
            "xanchor": "center",
            "yanchor": "top",
            "font": {"size": 13, "color": "#eaf2ff"},
            "bgcolor": "rgba(8, 17, 28, 0.75)",
        },
        xaxis_title="Mês",
        yaxis_title="Valor acumulado",
    )
    return apply_plotly_theme(fig)


def apply_discount(df: pd.DataFrame, discount_rate: float) -> pd.DataFrame:
    df = df.copy()
    if "valor" in df.columns:
        df["valor"] = df["valor"] * (1 - discount_rate)
    return df


def apply_filters(
    df: pd.DataFrame,
    data_inicio: datetime.date | None,
    data_fim: datetime.date | None,
    selected_secretaria: str,
    selected_combustivel: str,
) -> pd.DataFrame:
    df_f = df.copy()
    if data_inicio is not None:
        df_f = df_f[df_f["data_hora"].dt.date >= data_inicio]
    if data_fim is not None:
        df_f = df_f[df_f["data_hora"].dt.date <= data_fim]
    if selected_secretaria and selected_secretaria != "Todas":
        df_f = df_f[df_f["secretaria"].str.upper() == selected_secretaria.upper()]
    if selected_combustivel and selected_combustivel != "Todos":
        df_f = df_f[df_f["combustivel"].str.upper() == selected_combustivel.upper()]
    return df_f


def get_years_from_date_range(data_inicio: datetime.date | None, data_fim: datetime.date | None) -> list[int]:
	"""
	Calcula a lista de anos contidos no intervalo de datas fornecido.
	Se nenhuma data for fornecida, retorna o ano corrente.
	"""
	if data_inicio is None and data_fim is None:
		return [datetime.date.today().year]
	
	start = data_inicio or datetime.date(2020, 1, 1)
	end = data_fim or datetime.date.today()
	
	years = list(range(start.year, end.year + 1))
	return sorted(set(years))


def get_limits_df_for_period(data_inicio: datetime.date | None, data_fim: datetime.date | None) -> pd.DataFrame:
	"""
	Lê os parâmetros financeiros para os anos contidos no período filtrado.
	Se o período cruzar múltiplos anos, retorna registros para todos eles agrupados por secretaria.
	"""
	years = get_years_from_date_range(data_inicio, data_fim)
	df_limits = get_financial_params_by_years(years)
	return df_limits


def run_dashboard() -> None:
	_ensure_db_schema()
	st.set_page_config(page_title="Painel de Abastecimento", page_icon="⛽", layout="wide", initial_sidebar_state="expanded")
	_logo_path = Path(__file__).parent / "logo.svg"
	if _logo_path.exists():
		st.logo(str(_logo_path), size="large")
	inject_style()

	# Carregar desconto_rate inicial (será refreshado conforme período selecionado)
	discount_rate = get_discount_rate()
	if "db_version" not in st.session_state:
		st.session_state["db_version"] = 0

	# ── Importação de Excel (sempre visível na sidebar, independente do banco) ──
	def _render_sidebar_uploader():
		import re as _re, io as _io
		with st.sidebar:
			with st.expander("📂 Atualizar Dados", expanded=True):
				_up = st.file_uploader(
					"Importar relatório Excel",
					type=["xlsx", "xls"],
					help="Selecione o arquivo Relatorio.xlsx exportado do sistema.",
					key="upload_relatorio_early",
				)
				if _up is not None:
					_file_id = f"{_up.name}_{_up.size}"
					_imported_ids = st.session_state.get("_imported_ids", set())
					if _file_id not in _imported_ids:
						with st.spinner("Importando dados..."):
							try:
								_KNOWN_COLS = {"Placa", "Data/Hora", "Unidade", "Produto", "Qtde (L)", "Valor"}

								def _sanitize(name):
									s = name.strip().lower()
									s = _re.sub(r'\s+', '_', s)
									s = _re.sub(r'[^a-z0-9_]', '_', s)
									s = _re.sub(r'_+', '_', s).strip('_')
									return s or 'sheet'

								def _find_header_row(raw_df):
									for i, row in raw_df.iterrows():
										row_vals = set(str(v).strip() for v in row.values)
										if len(_KNOWN_COLS & row_vals) >= 2:
											return i
									return None

								def _safe_df(df):
									for _c in df.columns:
										if pd.api.types.is_datetime64_any_dtype(df[_c]):
											df[_c] = df[_c].astype(str)
										elif df[_c].dtype == object:
											df[_c] = df[_c].apply(lambda v: str(v) if hasattr(v, 'isoformat') else v)
									return df

								_file_bytes = _up.read()
								sheets_raw = pd.read_excel(_io.BytesIO(_file_bytes), sheet_name=None, header=None)
								single_sheet = len(sheets_raw) == 1
								used_tables: set = set()
								with sqlite3.connect(str(DB_PATH)) as _conn:
									for sheet_name, raw in sheets_raw.items():
										base = 'abastecimentos' if single_sheet else _sanitize(sheet_name)
										tbl = base
										idx = 1
										while tbl in used_tables:
											idx += 1
											tbl = f"{base}_{idx}"
										used_tables.add(tbl)
										header_row = _find_header_row(raw)
										if header_row is not None:
											df_import = pd.read_excel(_io.BytesIO(_file_bytes), sheet_name=sheet_name, header=header_row)
										else:
											df_import = raw
										try:
											df_existing = pd.read_sql_query(f'SELECT * FROM "{tbl}"', _conn)
										except Exception:
											df_existing = pd.DataFrame()
										df_combined = pd.concat([df_existing, df_import], ignore_index=True).drop_duplicates()
										_safe_df(df_combined).to_sql(tbl, _conn, if_exists='replace', index=False)
								_imported_ids.add(_file_id)
								st.session_state["_imported_ids"] = _imported_ids
								st.session_state["db_version"] = st.session_state.get("db_version", 0) + 1
								get_real_df.clear()
								st.success(f"✅ {_up.name} importado com sucesso! Recarregando...")
								st.rerun()
							except Exception as _e:
								import traceback as _tb
								st.error(f"❌ Erro ao importar: {_e}\n\n```\n{_tb.format_exc()}\n```")
					else:
						st.info("ℹ️ Este arquivo já foi importado nesta sessão.")

				st.divider()
				st.markdown("**🌐 Ou buscar via API**")
				_col1, _col2 = st.columns(2)
				with _col1:
					_early_di = st.date_input("De", value=datetime.date(datetime.date.today().year, 1, 1), format="DD/MM/YYYY", key="api_early_di")
				with _col2:
					_early_df = st.date_input("Até", value=datetime.date.today(), format="DD/MM/YYYY", key="api_early_df")
				if st.button("🌐 Buscar dados da API", key="btn_api_early", use_container_width=True):
					import json as _json2, urllib.request as _urllib_req2
					try:
						_cfg2 = _json2.loads((BASE_DIR / "config.json").read_text(encoding="utf-8"))
						_api2 = _cfg2.get("api_sisatec", {})
						_di2 = _early_di.strftime("%m-%d-%Y")
						_df2 = _early_df.strftime("%m-%d-%Y")
						_url2 = f"{_api2['base_url']}/{_api2['codigo']}/{_api2['key']}/{_di2}/{_df2}"

						def _fetch_all_pages(base_url):
							def _get(pg):
								_u = base_url if pg == 1 else f"{base_url}?pagina={pg}"
								_req = _urllib_req2.Request(_u, headers={"Accept": "application/json"})
								with _urllib_req2.urlopen(_req, timeout=30) as _r:
									return _json2.loads(_r.read().decode("utf-8"))
							_first = _get(1)
							if isinstance(_first, list):
								return _first
							_d = _first.get("abastecimentos", _first.get("dados", _first.get("data", _first.get("registros", []))))
							_total = int(_first.get("total_paginas", _first.get("totalPaginas", 1)) or 1)
							_all = list(_d)
							for _pg in range(2, _total + 1):
								_r = _get(_pg)
								_all.extend(_r.get("abastecimentos", _r.get("dados", _r.get("data", _r.get("registros", [])))))
							return _all

						def _br_float2(v):
							if v is None: return None
							try: return float(str(v).replace(",", "."))
							except: return None

						def _records_to_df2(records):
							rows = []
							for r in records:
								_data = str(r.get("data", "") or "").strip()
								_hora = str(r.get("hora", "") or "").strip()
								try:
									_data_iso = datetime.datetime.strptime(_data, "%d/%m/%Y").strftime("%Y-%m-%d")
								except Exception:
									_data_iso = _data
								_kma = _br_float2(r.get("kmAtual"))
								_kman = _br_float2(r.get("kmAnterior"))
								_krod_api = _br_float2(r.get("KmHoraRodado"))
								_krod = _krod_api if (_krod_api and _krod_api > 0) else ((_kma - _kman) if (_kma and _kman and _kma > _kman) else None)
								_lit = _br_float2(r.get("quantidadeLitros"))
								_kml_api = _br_float2(r.get("KmHoraPorLitro"))
								_kml = _kml_api if (_kml_api and _kml_api > 0) else ((_krod / _lit) if (_krod and _lit and _lit > 0) else None)
								_val = _br_float2(r.get("valor"))
								rows.append({
									"Data/Hora": f"{_data_iso} {_hora}".strip() if _data_iso else None,
									"Placa": str(r.get("placa", "") or "").upper().strip(),
									"Condutor": str(r.get("condutor", "") or "").strip(),
									"Marca": str(r.get("marca", "") or "").strip(),
									"Modelo": str(r.get("modelo", "") or "").strip(),
									"Ano": str(r.get("ano_veiculo", "") or "").strip(),
									"Ult. km": _kman, "km Atual": _kma, "km/L": _kml, "Km Rodado": _krod,
									"Qtde (L)": _lit, "Vr. Unit.": _br_float2(r.get("valorLitro")),
									"Valor": _val,
									"Produto": str(r.get("nomeServico", "") or r.get("combustivel", "") or "").strip(),
									"Unidade": str(r.get("centroDeCustoVeiculo", "") or "").strip(),
									"Estabelecimento": str(r.get("posto", "") or "").strip(),
									"Registro": str(r.get("registroCondutor", "") or "").strip(),
									"Prefixo": str(r.get("prefixo", "") or "").strip(),
									"Tipo Frota": str(r.get("TipoFrota", "") or "").strip(),
									"R$/km": (_val / _krod if (_val and _krod and _krod > 0) else None),
								})
							return pd.DataFrame(rows)

						with st.spinner("Consultando API..."):
							_todos2 = _fetch_all_pages(_url2)
						if not _todos2:
							st.info("ℹ️ Nenhum registro encontrado no período.")
						else:
							with st.spinner(f"Importando {len(_todos2)} registros..."):
								_df_api2 = _records_to_df2(_todos2)
								with sqlite3.connect(str(DB_PATH)) as _conn2:
									try:
										_df_ex2 = pd.read_sql_query('SELECT * FROM "abastecimentos"', _conn2)
									except Exception:
										_df_ex2 = pd.DataFrame()
									_df_mg2 = pd.concat([_df_ex2, _df_api2], ignore_index=True).drop_duplicates(
										subset=["Data/Hora", "Placa", "Qtde (L)", "Valor"], keep="first"
									)
									_df_mg2.to_sql("abastecimentos", _conn2, if_exists="replace", index=False)
								st.session_state["db_version"] = st.session_state.get("db_version", 0) + 1
								get_real_df.clear()
								st.success(f"✅ {len(_todos2)} registros importados via API! Recarregando...")
								st.rerun()
					except Exception as _e2:
						import traceback as _tb2
						st.error(f"❌ Erro na API: {_e2}\n\n```\n{_tb2.format_exc()}\n```")

	try:
		df_real = apply_discount(get_real_df(st.session_state["db_version"]), discount_rate)
	except RuntimeError as _e:
		if "Nenhuma tabela de abastecimento" in str(_e):
			st.error(
				"⚠️ **Banco de dados desatualizado ou incompatível.**\n\n"
				"O arquivo `relatorio.db` existe, mas não contém a tabela esperada de abastecimentos. "
				"Use o botão **📂 Atualizar Dados** na barra lateral para reimportar o relatório."
			)
		else:
			st.error(f"Erro ao carregar dados: {_e}")
		_render_sidebar_uploader()
		st.stop()

	# Opções dos selectboxes
	# Carregar secretarias do banco para todas os anos disponíveis (para o selectbox)
	df_all_limits = get_financial_params_by_years([2024, 2025, 2026])
	secretaria_options = ["Todas"] + sorted(df_all_limits["secretaria"].dropna().unique().tolist())
	combustivel_options = ["Todos"] + sorted(df_real["combustivel"].dropna().unique().tolist())

	# Datas disponíveis no banco
	_raw_min = df_real["data_hora"].dt.date.min() if ("data_hora" in df_real.columns and not df_real.empty) else None
	_raw_max = df_real["data_hora"].dt.date.max() if ("data_hora" in df_real.columns and not df_real.empty) else None
	data_min_db = _raw_min if (isinstance(_raw_min, datetime.date) and not pd.isnull(_raw_min)) else datetime.date(2024, 1, 1)
	data_max = _raw_max if (isinstance(_raw_max, datetime.date) and not pd.isnull(_raw_max)) else datetime.date.today()
	# Padrão: ano corrente (01/01/ano_atual até hoje)
	_ano_corrente = datetime.date.today().year
	_default_ini = max(datetime.date(_ano_corrente, 1, 1), data_min_db)
	_default_fim = data_max

	with st.sidebar:
		# ── Filtros ──
		with st.expander("🔍 Filtros", expanded=False):
			_periodo = st.date_input(
				"Período",
				value=(_default_ini, _default_fim),
				min_value=data_min_db,
				max_value=data_max,
				format="DD/MM/YYYY",
				key="sel_periodo",
			)
			data_inicio = _periodo[0] if isinstance(_periodo, (list, tuple)) and len(_periodo) >= 1 else None
			data_fim = _periodo[1] if isinstance(_periodo, (list, tuple)) and len(_periodo) == 2 else None
			selected_secretaria = st.selectbox("Unidade / Secretaria", secretaria_options, index=0, key="sel_sec")
			selected_combustivel = st.selectbox("Produto (Combustível)", combustivel_options, index=0, key="sel_comb")

			if st.button("🗑️ Limpar Filtros", use_container_width=True):
				for k in ("sel_periodo", "sel_sec", "sel_comb"):
					if k in st.session_state:
						del st.session_state[k]
				st.rerun()

		# ── Alertas (computados com escopo do ano corrente, todas secretarias) ──
		_ano_ini = datetime.date(datetime.date.today().year, 1, 1)
		df_alert_base = apply_filters(df_real, _ano_ini, datetime.date.today(), "Todas", "Todos")
		# Carregar limites para o ano corrente (para alertas)
		df_limits_for_alerts = get_limits_df_for_period(_ano_ini, datetime.date.today())
		status_alerts = build_secretaria_status(df_alert_base, df_limits_for_alerts)

		excedidas = status_alerts[status_alerts["status"].isin(["ESTOURO POR PRECO", "ESTOURO GERAL"])].copy() if "status" in status_alerts.columns else pd.DataFrame()
		proximas = status_alerts[
			(status_alerts["status"] == "OK") &
			(status_alerts["desvio_pct"] >= -20) & (status_alerts["desvio_pct"] < 0)
		].copy() if "desvio_pct" in status_alerts.columns else pd.DataFrame()
		ok = status_alerts[
			(status_alerts["status"] == "OK") &
			(status_alerts["desvio_pct"] < -20)
		].copy() if "status" in status_alerts.columns else pd.DataFrame()

		n_exc = len(excedidas)
		n_prox = len(proximas)
		n_ok = len(ok)

		months_alert = month_count(df_alert_base)
		periodo_label = f"1 mês" if months_alert == 1 else f"{months_alert} meses"

		# Excedidas: mostra só o nome da secretaria
		if n_exc and "secretaria" in excedidas.columns:
			_exc_parts = [row['secretaria'] for _, row in excedidas.head(3).iterrows()]
			if n_exc > 3:
				_exc_parts.append(f"+ {n_exc - 3} mais")
			exc_names = "<br>".join(_exc_parts)
		else:
			exc_names = ""

		# Próximas: mostra só o nome da secretaria
		if n_prox and "secretaria" in proximas.columns:
			_prox_parts = [row['secretaria'] for _, row in proximas.head(3).iterrows()]
			if n_prox > 3:
				_prox_parts.append(f"+ {n_prox - 3} mais")
			prox_names = "<br>".join(_prox_parts)
		else:
			prox_names = ""

		st.markdown('<div class="sidebar-section">ALERTAS</div>', unsafe_allow_html=True)
		st.markdown(
			f"""
			<div class="alert-card alert-red">
				<span class="alert-icon">🔴</span>
				<div class="alert-body">
					<span class="alert-count-red">{n_exc}</span>
					<span class="alert-label">Limite acumulado excedido</span>
					<span class="alert-detail">{exc_names}</span>
				</div>
			</div>
			<div class="alert-card alert-yellow">
				<span class="alert-icon">🟡</span>
				<div class="alert-body">
					<span class="alert-count-yellow">{n_prox}</span>
					<span class="alert-label">Próximo do limite (&lt;20% restante)</span>
					<span class="alert-detail">{prox_names}</span>
				</div>
			</div>
			<div class="alert-card alert-green">
				<span class="alert-icon">🟢</span>
				<div class="alert-body">
					<span class="alert-count-green">{n_ok}</span>
					<span class="alert-label">Dentro do limite</span>
				</div>
			</div>
			""",
			unsafe_allow_html=True,
		)

		# ── Atualizar Dados ──
		with st.expander("📂 Atualizar Dados", expanded=False):
			uploaded = st.file_uploader(
				"Importar relatório Excel",
				type=["xlsx", "xls"],
				help="Selecione o arquivo Relatorio.xlsx exportado do sistema.",
				key="upload_relatorio",
			)
			if uploaded is not None:
				# Identificar arquivo por nome+tamanho (evita reimportar o mesmo)
				_file_id = f"{uploaded.name}_{uploaded.size}"
				_imported_ids = st.session_state.get("_imported_ids", set())
				_force = st.session_state.pop("_force_reimport", False)

				if _file_id not in _imported_ids or _force:
					with st.spinner("Importando dados..."):
						try:
							import re as _re, io as _io
							_KNOWN_COLS = {"Placa", "Data/Hora", "Unidade", "Produto", "Qtde (L)", "Valor"}

							def _sanitize(name):
								s = name.strip().lower()
								s = _re.sub(r'\s+', '_', s)
								s = _re.sub(r'[^a-z0-9_]', '_', s)
								s = _re.sub(r'_+', '_', s).strip('_')
								return s or 'sheet'

							def _find_header_row(raw_df):
								for i, row in raw_df.iterrows():
									row_vals = set(str(v).strip() for v in row.values)
									if len(_KNOWN_COLS & row_vals) >= 2:
										return i
								return None

							def _safe_df(df):
								"""Converte colunas Timestamp para string antes de salvar no SQLite."""
								for _c in df.columns:
									if pd.api.types.is_datetime64_any_dtype(df[_c]):
										df[_c] = df[_c].astype(str)
									elif df[_c].dtype == object:
										df[_c] = df[_c].apply(
											lambda v: str(v) if hasattr(v, 'isoformat') else v
										)
								return df

							_file_bytes = uploaded.read()
							sheets_raw = pd.read_excel(_io.BytesIO(_file_bytes), sheet_name=None, header=None)
							single_sheet = len(sheets_raw) == 1
							used_tables = set()
							with sqlite3.connect(str(DB_PATH)) as _conn:
								for sheet_name, raw in sheets_raw.items():
									base = 'abastecimentos' if single_sheet else _sanitize(sheet_name)
									tbl = base
									idx = 1
									while tbl in used_tables:
										idx += 1
										tbl = f"{base}_{idx}"
									used_tables.add(tbl)

									header_row = _find_header_row(raw)
									if header_row is not None:
										df_import = pd.read_excel(
											_io.BytesIO(_file_bytes),
											sheet_name=sheet_name,
											header=header_row,
										)
									else:
										df_import = raw

									# Mesclar com dados existentes (não sobrescrever)
									try:
										df_existing = pd.read_sql_query(f'SELECT * FROM "{tbl}"', _conn)
									except Exception:
										df_existing = pd.DataFrame()

									df_combined = pd.concat([df_existing, df_import], ignore_index=True)
									df_combined = df_combined.drop_duplicates()
									df_combined = _safe_df(df_combined)
									df_combined.to_sql(tbl, _conn, if_exists='replace', index=False)

							_imported_ids.add(_file_id)
							st.session_state["_imported_ids"] = _imported_ids
							st.session_state["db_version"] = st.session_state.get("db_version", 0) + 1
							get_real_df.clear()  # força releitura do banco mesmo se db_version resetar
							for _k in ("sel_ano", "sel_mes", "sel_sec", "sel_comb"):
								st.session_state.pop(_k, None)
							st.success(f"✅ {uploaded.name} importado com sucesso! Recarregando...")
							st.rerun()
						except Exception as _e:
							import traceback as _tb
							st.error(f"❌ Erro ao importar: {_e}\n\n```\n{_tb.format_exc()}\n```")
				else:
					st.info("ℹ️ Este arquivo já foi importado nesta sessão.")
					if st.button("🔄 Forçar reimportação", key="force_reimport"):
						st.session_state["_force_reimport"] = True
						st.rerun()

			st.divider()

			# ── Atualizar via API ──
			st.markdown("**🌐 Atualizar via API**")
			# Usar o timestamp completo (não só a data) para não perder registros do mesmo dia
			_api_ultimo_ts = df_real["data_hora"].max() if ("data_hora" in df_real.columns and not df_real.empty) else None
			if pd.isna(_api_ultimo_ts) if _api_ultimo_ts is not None else True:
				_api_ultimo_ts = None
			if _api_ultimo_ts is not None:
				_api_hoje = datetime.date.today()
				# dataInicio = mesmo dia do último registro (pega registros posteriores do mesmo dia)
				_api_di_date = _api_ultimo_ts.date()
				_api_ultimo_str = _api_ultimo_ts.strftime("%d/%m/%Y %H:%M")
				st.caption(f"Última atualização: **{_api_ultimo_str}**")
				st.caption(
					f"Buscando registros após **{_api_ultimo_str}** até **{_api_hoje.strftime('%d/%m/%Y')}**"
				)
				if st.button("🌐 Buscar dados da API", key="btn_api_update", use_container_width=True):
					import json as _json, urllib.request as _urllib_req
					try:
						_cfg = _json.loads((BASE_DIR / "config.json").read_text(encoding="utf-8"))
						_api_cfg = _cfg.get("api_sisatec", {})
						_codigo = _api_cfg.get("codigo", "")
						_key = _api_cfg.get("key", "")
						_base_url = _api_cfg.get("base_url", "")
						_di = _api_di_date.strftime("%m-%d-%Y")
						_df_str = _api_hoje.strftime("%m-%d-%Y")

						def _buscar_pagina(pagina: int) -> dict:
							_url = f"{_base_url}/{_codigo}/{_key}/{_di}/{_df_str}?pagina={pagina}"
							_req = _urllib_req.Request(_url, headers={"Accept": "application/json"})
							with _urllib_req.urlopen(_req, timeout=30) as _resp:
								return _json.loads(_resp.read().decode("utf-8"))

						def _br_float(v):
							if v is None:
								return None
							try:
								return float(str(v).replace(",", "."))
							except Exception:
								return None

						def _api_records_to_df(records: list) -> pd.DataFrame:
							rows = []
							for r in records:
								_data = str(r.get("data", "") or "").strip()
								_hora = str(r.get("hora", "") or "").strip()
								# Converter DD/MM/YYYY → YYYY-MM-DD para manter formato ISO uniforme no banco
								try:
									_data_iso = datetime.datetime.strptime(_data, "%d/%m/%Y").strftime("%Y-%m-%d")
								except Exception:
									_data_iso = _data
								_dt = f"{_data_iso} {_hora}".strip() if _data_iso else None
								_km_atual = _br_float(r.get("kmAtual"))
								_km_ant = _br_float(r.get("kmAnterior"))
								_km_rod_api = _br_float(r.get("KmHoraRodado"))
								_km_rod = _km_rod_api if (_km_rod_api and _km_rod_api > 0) else (
									(_km_atual - _km_ant) if (_km_atual and _km_ant and _km_atual > _km_ant) else None
								)
								_kml_api = _br_float(r.get("KmHoraPorLitro"))
								_litros = _br_float(r.get("quantidadeLitros"))
								_kml = _kml_api if (_kml_api and _kml_api > 0) else (
									(_km_rod / _litros) if (_km_rod and _litros and _litros > 0) else None
								)
								rows.append({
									"Data/Hora":           _dt,
									"Placa":               str(r.get("placa", "") or "").upper().strip(),
									"Condutor":            str(r.get("condutor", "") or "").strip(),
									"Marca":               str(r.get("marca", "") or "").strip(),
									"Modelo":              str(r.get("modelo", "") or "").strip(),
									"Ano":                 str(r.get("ano_veiculo", "") or "").strip(),
									"Ult. km":             _km_ant,
									"km Atual":            _km_atual,
									"km/L":                _kml,
									"Km Rodado":           _km_rod,
									"Qtde (L)":            _litros,
									"Vr. Unit.":           _br_float(r.get("valorLitro")),
									"Valor":               _br_float(r.get("valor")),
									"Produto":             str(r.get("nomeServico", "") or r.get("combustivel", "") or "").strip(),
									"Unidade":             str(r.get("centroDeCustoVeiculo", "") or "").strip(),
									"Estabelecimento":     str(r.get("posto", "") or "").strip(),
									"Registro":            str(r.get("registroCondutor", "") or "").strip(),
									"Prefixo":             str(r.get("prefixo", "") or "").strip(),
									"Tipo Frota":          str(r.get("TipoFrota", "") or "").strip(),
									"R$/km":               ((_br_float(r.get("valor")) / _km_rod) if (_br_float(r.get("valor")) and _km_rod and _km_rod > 0) else None),
								})
							return pd.DataFrame(rows)

						with st.spinner("Consultando API..."):
							_primeira = _buscar_pagina(1)
							if isinstance(_primeira, list):
								_todos = _primeira
							else:
								_dados = _primeira.get("abastecimentos", _primeira.get("dados", _primeira.get("data", _primeira.get("registros", []))))
								_total_pag = int(_primeira.get("total_paginas", _primeira.get("totalPaginas", 1)) or 1)
								_todos = list(_dados)
								for _pag in range(2, _total_pag + 1):
									_resp_pag = _buscar_pagina(_pag)
									_d = _resp_pag.get("abastecimentos", _resp_pag.get("dados", _resp_pag.get("data", _resp_pag.get("registros", []))))
									_todos.extend(_d)

						if not _todos:
							st.info("ℹ️ Nenhum registro novo encontrado no período.")
						else:
							with st.spinner(f"Importando {len(_todos)} registros..."):
								_df_api = _api_records_to_df(_todos)
								# Filtrar apenas registros com timestamp POSTERIOR ao último já existente
								# A API retorna o dia inteiro de _api_di_date, então descartamos duplicatas de horário
								_df_api["_dt_parsed"] = pd.to_datetime(
									_df_api["Data/Hora"], dayfirst=True, errors="coerce"
								)
								_df_api = _df_api[_df_api["_dt_parsed"] > _api_ultimo_ts].drop(columns=["_dt_parsed"])
								if _df_api.empty:
									st.info("ℹ️ Nenhum registro novo após o último horário importado.")
								else:
									with sqlite3.connect(str(DB_PATH)) as _conn_api:
										try:
											_df_exist = pd.read_sql_query('SELECT * FROM "abastecimentos"', _conn_api)
										except Exception:
											_df_exist = pd.DataFrame()
										_df_merged = pd.concat([_df_exist, _df_api], ignore_index=True).drop_duplicates(
											subset=["Data/Hora", "Placa", "Qtde (L)", "Valor"],
											keep="first",
										)
										_df_merged.to_sql("abastecimentos", _conn_api, if_exists="replace", index=False)
									st.session_state["db_version"] = st.session_state.get("db_version", 0) + 1
									get_real_df.clear()
									st.success(f"✅ {len(_df_api)} registros novos importados via API! Recarregando...")
									st.rerun()
					except Exception as _e_api:
						import traceback as _tb_api
						st.error(f"❌ Erro na API: {_e_api}\n\n```\n{_tb_api.format_exc()}\n```")
			else:
				st.caption("Sem dados no banco para determinar o período. Importe um Excel primeiro.")

		render_financial_params_editor_sidebar()

		# ── Rodapé ──
		ultima_atualizacao = data_max.strftime("%d/%m/%Y") if (data_max and isinstance(data_max, datetime.date)) else "—"
		st.markdown(
			f"""<div class="sidebar-footer">
			🕐 Última atualização: <b>{ultima_atualizacao}</b><br>
			Base: {DB_PATH.name} &nbsp;|&nbsp; Desconto: {discount_rate*100:.2f}%
			</div>""",
			unsafe_allow_html=True,
		)

	# ── Filtrar dados conforme seleção ──
	filtered = apply_filters(df_real, data_inicio, data_fim, selected_secretaria, selected_combustivel)

	# Carregar limites dinamicamente conforme período selecionado
	df_limits = get_limits_df_for_period(data_inicio, data_fim)
	# Gráfico anual comparativo sempre com visão global (todos os anos, sem filtros de sidebar)
	anual_scope = apply_filters(df_real, None, None, "Todas", "Todos")
	yoy_scope = apply_filters(df_real, None, None, selected_secretaria, selected_combustivel)

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

	# ── Cabeçalho principal ──
	if data_inicio and data_fim:
		ctx_parts = [f"{data_inicio.strftime('%d/%m/%Y')} → {data_fim.strftime('%d/%m/%Y')}"]
	elif data_inicio:
		ctx_parts = [f"A partir de {data_inicio.strftime('%d/%m/%Y')}"]
	else:
		ctx_parts = ["Todo o período"]
	if selected_secretaria != "Todas":
		ctx_parts.append(selected_secretaria)
	if selected_combustivel != "Todos":
		ctx_parts.append(selected_combustivel)
	filtro_ctx = " · ".join(ctx_parts)
	st.markdown(
		f"""<div style="display:flex;align-items:baseline;gap:16px;margin-bottom:0.5rem;padding-bottom:0.4rem;border-bottom:1px solid rgba(142,163,190,0.18);">
		<span style="font-family:'Rajdhani',sans-serif;font-size:2rem;font-weight:700;color:#e7eef8;letter-spacing:0.02em;">DASHBOARD DE ABASTECIMENTO</span>
		<span style="font-size:0.88rem;color:#8ea3be;font-family:'Space Grotesk',sans-serif;">Análise completa da frota &nbsp;•&nbsp; {filtro_ctx}</span>
		</div>""",
		unsafe_allow_html=True,
	)

	# ── Calcular deltas YoY para os KPI cards ──
	_yoy_deltas: dict | None = None
	if data_inicio and data_fim:
		try:
			_yoy_ini = data_inicio.replace(year=data_inicio.year - 1)
			_yoy_fim = data_fim.replace(year=data_fim.year - 1)
		except ValueError:
			_yoy_ini = data_inicio.replace(year=data_inicio.year - 1, day=28)
			_yoy_fim = data_fim.replace(year=data_fim.year - 1, day=28)
		_filtered_yoy = apply_filters(df_real, _yoy_ini, _yoy_fim, selected_secretaria, selected_combustivel)
		if not _filtered_yoy.empty:
			_gasto_yoy = float(_filtered_yoy["valor"].sum())
			_litros_yoy = float(_filtered_yoy["litros"].sum()) if "litros" in _filtered_yoy.columns else 0.0
			_n_yoy = len(_filtered_yoy)
			_ano_ref = _yoy_ini.year
			_months_yoy = month_count(_filtered_yoy)
			_media_yoy = _gasto_yoy / _months_yoy if _months_yoy else 0.0
			_consumo_yoy = float(_filtered_yoy["km_por_litro"].mean()) if "km_por_litro" in _filtered_yoy.columns and _filtered_yoy["km_por_litro"].notna().any() else 0.0
			_custo_km_yoy = float(_filtered_yoy["custo_por_km"].mean()) if "custo_por_km" in _filtered_yoy.columns and _filtered_yoy["custo_por_km"].notna().any() else 0.0
			_veiculos_yoy = _filtered_yoy["placa"].nunique() if "placa" in _filtered_yoy.columns else 0
			# Empenho do ano anterior (independe de ter dados de abastecimento no período YoY)
			_limits_prev = get_financial_params_by_years([_yoy_ini.year])
			if selected_secretaria != "Todas":
				_limits_prev = _limits_prev[_limits_prev["secretaria"] == normalize_secretaria(selected_secretaria)]
			_empenhado_yoy = float(_limits_prev["valor_empenhado"].sum()) if not _limits_prev.empty else 0.0
			def _pct_delta(cur, prev):
				return (cur - prev) / prev * 100 if prev else None
			_yoy_deltas = {
				"valor_empenhado": (_pct_delta(kpis["valor_empenhado"], _empenhado_yoy), _ano_ref) if _empenhado_yoy else None,
				"gasto_total": (_pct_delta(kpis["gasto_total"], _gasto_yoy), _ano_ref),
				"gasto_litros": (_pct_delta(kpis["gasto_litros"], _litros_yoy), _ano_ref) if _litros_yoy else None,
				"n_abastecimentos": (_pct_delta(kpis["n_abastecimentos"], _n_yoy), _ano_ref),
				"media_mensal_consumo": (_pct_delta(kpis["media_mensal_consumo"], _media_yoy), _ano_ref) if _media_yoy else None,
				"consumo_medio": (_pct_delta(kpis["consumo_medio"], _consumo_yoy), _ano_ref) if _consumo_yoy else None,
				"custo_por_km": (_pct_delta(kpis["custo_por_km"], _custo_km_yoy), _ano_ref) if _custo_km_yoy else None,
				"veiculos_ativos": (_pct_delta(kpis["veiculos_ativos"], _veiculos_yoy), _ano_ref) if _veiculos_yoy else None,
			}
			for _k in ("valor_empenhado", "gasto_litros", "media_mensal_consumo", "consumo_medio", "custo_por_km", "veiculos_ativos"):
				if _yoy_deltas.get(_k) is None:
					_yoy_deltas.pop(_k, None)
	render_kpi_cards(kpis, deltas=_yoy_deltas)
	st.caption(
		f"Valores com desconto contratual de {discount_rate * 100:.2f}% aplicado sobre o valor bruto."
	)

	tab_fin, tab_con, tab_sec, tab_veic, tab_postos = st.tabs(["📊 Financeiro", "⛽ Consumo", "🏢 Secretarias", "🚗 Veículos", "⛽ Postos"])

	with tab_fin:
		col_ano, col_mes = st.columns([1, 2])
		col_ano.plotly_chart(
			make_bar_gasto_por_ano(anual_scope, selected_secretaria, selected_combustivel, discount_rate),
			use_container_width=True, key="bar_gasto_ano",
		)
		col_mes.plotly_chart(
			make_bar_gasto_por_mes_unificado(filtered, selected_secretaria, selected_combustivel),
			use_container_width=True, key="bar_gasto_mes_unificado",
		)
		st.plotly_chart(
			make_bar_comparativo_mensal_yoy(yoy_scope, data_inicio, data_fim),
			use_container_width=True,
			key="bar_comparativo_mensal_yoy",
		)
		bar_col, donut_col = st.columns([2, 1])
		bar_col.plotly_chart(make_bar_consumo_tipo_mes(filtered), use_container_width=True, key="bar_combustivel_fin")
		donut_col.plotly_chart(make_donut_combustivel_valor(filtered), use_container_width=True, key="donut_combustivel_valor")
		st.plotly_chart(
			make_line_real_previsto_projecao(
				filtered,
				limits_scope,
				usar_limite_quinzenal_secretaria=selected_secretaria != "Todas",
			),
			use_container_width=True,
			key="line_real_previsto",
		)
		st.plotly_chart(
			make_line_custo_medio_rl_combustivel(filtered),
			use_container_width=True,
			key="line_custo_medio_rl",
		)
		st.plotly_chart(
			make_bar_valor_vs_limite_secretaria(status),
			use_container_width=True,
			key="bar_valor_limite_sec",
		)

	with tab_con:
		bar_con_col, donut_con_col = st.columns([2, 1])
		bar_con_col.plotly_chart(make_bar_consumo_tipo_mes_litros(filtered), use_container_width=True, key="bar_combustivel_litros")
		donut_con_col.plotly_chart(make_donut_combustivel(filtered), use_container_width=True, key="donut_combustivel")
		st.plotly_chart(
			make_bar_comparativo_mensal_yoy_litros(yoy_scope, data_inicio, data_fim),
			use_container_width=True, key="bar_comp_mensal_litros",
		)
		st.plotly_chart(
			make_line_custo_medio_mes_combustivel(filtered),
			use_container_width=True, key="line_custo_medio",
		)


	with tab_sec:
		st.plotly_chart(make_ranking_consumo_secretaria(status), use_container_width=True, key="bar_ranking_sec")
		st.plotly_chart(make_bar_consumo_secretaria(status, df_limits), use_container_width=True, key="bar_sec")
		# Tabela de alertas detalhada
		if not excedidas.empty:
			st.markdown('#### 🔴 Secretarias com Limite Excedido')
			cols_show = [c for c in ("secretaria", "gasto_valor", "limite_valor_periodo", "desvio_pct", "desvio_valor") if c in excedidas.columns]
			st.dataframe(excedidas[cols_show].rename(columns={
				"secretaria": "Secretaria",
				"gasto_valor": "Gasto (R$)",
				"limite_valor_periodo": "Limite (R$)",
				"desvio_pct": "Desvio (%)",
				"desvio_valor": "Desvio (R$)",
			}), use_container_width=True)

	with tab_veic:
		st.plotly_chart(make_ranking_veiculos(filtered), use_container_width=True, key="ranking_veic_valor")

	with tab_postos:
		st.plotly_chart(make_treemap_postos(filtered), use_container_width=True, key="treemap_postos")
		# Tabela complementar
		if not filtered.empty and "posto" in filtered.columns:
			df_postos_tbl = (
				filtered.groupby("posto", as_index=False)
				.agg(valor=("valor", "sum"), litros=("litros", "sum"), abastecimentos=("placa", "count"))
				.sort_values("valor", ascending=False)
				.rename(columns={"posto": "Posto", "valor": "Gasto (R$)", "litros": "Litros", "abastecimentos": "Abastecimentos"})
			)
			st.dataframe(df_postos_tbl.reset_index(drop=True), use_container_width=True)

if __name__ == "__main__":
	run_dashboard()

