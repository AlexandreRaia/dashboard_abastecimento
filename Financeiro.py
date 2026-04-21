import json
import sqlite3
import datetime
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from plotly_utils import apply_plotly_theme
from make_bar_consumo_secretaria import make_bar_consumo_secretaria

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "relatorio.db"
CONFIG_PATH = BASE_DIR / "config.json"
DEFAULT_DISCOUNT_RATE = 0.0405

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

	# Calcular meta mensal
	_limits_df = get_limits_df()
	if selected_secretaria and selected_secretaria != "Todas":
		_row = _limits_df[_limits_df["secretaria"].str.upper() == selected_secretaria.upper()]
		meta_mensal = float(_row["empenho_2026"].iloc[0]) / 12 if not _row.empty else 0.0
	else:
		meta_mensal = float(_limits_df["empenho_2026"].sum()) / 12

	# Corrigir todos os labels e agregar valores por mês/ano único
	monthly_totals["periodo_corrigido"] = monthly_totals["periodo"].apply(corrige_mes_nome)
	agrupado = monthly_totals.groupby(["ano", "mes", "periodo_corrigido"], as_index=False).agg({
		"valor_total_mes": "sum",
		"litros_total_mes": "sum",
		"variacao_pct": "first"  # ou média, se preferir
	})
	# Recalcular azul/vermelho após agregação
	agrupado["azul"] = agrupado["valor_total_mes"].clip(upper=meta_mensal)
	agrupado["vermelho"] = (agrupado["valor_total_mes"] - meta_mensal).clip(lower=0)
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
			text=[f"R$ {t:,.2f}" for t in total_bar],
			textposition="top center",
			showlegend=False,
			textfont={"size": 12, "color": "#fff", "family": "'Space Grotesk', sans-serif"},
			hoverinfo="skip",
			texttemplate="<span style='text-shadow: -2px -2px 0 #222, 2px -2px 0 #222, -2px 2px 0 #222, 2px 2px 0 #222;'>%{text}</span>",
		)
	)
	# Limite mensal já calculado como meta_mensal
	limite_mensal = meta_mensal

	# Adiciona linha tracejada horizontal do limite
	if limite_mensal > 0:
		fig.add_shape(
			type="line",
			x0=-0.5,
			x1=len(monthly_totals["periodo"]) - 0.5,
			y0=limite_mensal,
			y1=limite_mensal,
			line=dict(color="#eab308", width=3, dash="dash"),
			xref="x",
			yref="y",
			layer="above"
		)
		# Adiciona uma scatter invisível para a legenda
		fig.add_trace(
			go.Scatter(
				x=[None],
				y=[None],
				mode="lines",
				line=dict(color="#eab308", width=3, dash="dash"),
				name="Limite mensal"
			)
		)

	fig.update_layout(
		template="plotly_dark",
		title="Consumo Combustível por mês",
		xaxis_title="Período",
		yaxis_title="Valor faturado",
		margin={"l": 30, "r": 30, "t": 110, "b": 30},  # aumenta o topo para dar espaço
		bargap=0.45,
		barmode="stack",
		legend={"orientation": "h", "x": 0.01, "y": 1.02, "xanchor": "left", "yanchor": "bottom"},
	)
	return apply_plotly_theme(fig)


def make_bar_gasto_por_ano(df: pd.DataFrame, selected_secretaria: str = "Todas", selected_combustivel: str = "Todos") -> go.Figure:
    if df.empty or not {'ano', 'valor'}.issubset(df.columns):
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados para gasto anual")
        return apply_plotly_theme(fig)

    grupo = df.groupby("ano", as_index=False).agg(valor_total=("valor", "sum"))
    grupo = grupo.sort_values("ano")
    fig = go.Figure(go.Bar(
        x=grupo["ano"].astype(str),
        y=grupo["valor_total"],
        marker_color=["#2563eb" if int(a)==2026 else "#38bdf8" for a in grupo["ano"]],
        text=[f"R$ {v:,.2f}" for v in grupo["valor_total"]],
        textposition="outside",
        textfont={"size": 16, "color": "#fff", "family": "'Space Grotesk', sans-serif"},
        name="Gasto anual"
    ))
    fig.update_layout(
        template="plotly_dark",
        title={"text": "Gasto anual comparativo", "font": {"size": 20, "color": "#eaf2ff"}},
        xaxis_title="Ano",
        yaxis_title="Valor total (R$)",
        margin={"l": 20, "r": 20, "t": 60, "b": 30},
        legend={"font": {"size": 14, "color": "#eaf2ff"}},
    )
    return apply_plotly_theme(fig)


def make_donut_combustivel(df_filtered: pd.DataFrame) -> go.Figure:
	if df_filtered.empty:
		fig = go.Figure()
		fig.update_layout(template="plotly_dark", title="Sem dados por combustível")
		return apply_plotly_theme(fig)

	by_fuel = df_filtered.groupby("combustivel", as_index=False).agg(litros=("litros", "sum"))
	color_map = {"GASOLINA": "#2563eb", "ALCOOL": "#ff7f0e", "DIESEL": "#fb7185", "DIESEL S10": "#38bdf8"}
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
		title="Mix de combustível por volume (Litros)",
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
	color_map = {"GASOLINA": "#2563eb", "ALCOOL": "#ff7f0e", "DIESEL": "#fb7185", "DIESEL S10": "#38bdf8"}
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
		title="Mix de combustível por valor (R$)",
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
			font-size: 1.35rem;
			font-weight: 700;
			margin: 0.45rem 0 0.25rem 0;
			color: #d9e3f0;
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
		.alert-detail { font-size: 0.65rem; color: #8ea3be; }

		/* ── Última atualização ── */
		.sidebar-footer {
			font-size: 0.68rem;
			color: #8ea3be;
			margin-top: 10px;
			padding-top: 8px;
			border-top: 1px solid rgba(142,163,190,0.15);
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
	df = load_config(CONFIG_PATH)[0]
	# Remove campo legado se existir
	if "limite_quinzenal" in df.columns:
		df = df.drop(columns=["limite_quinzenal"])
	return df


@st.cache_data(show_spinner=False)
def get_discount_rate() -> float:
	return load_config(CONFIG_PATH)[1]


@st.cache_data(show_spinner=False)
def get_real_df(cache_version: str = "v2_valor_unitario") -> pd.DataFrame:
	_ = cache_version
	return load_sqlite(DB_PATH)




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
            "limite_mensal": float(item.get("limite_mensal", 0.0)),
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
    months = month_count(df_filtered)

    real = (
        df_filtered.groupby("secretaria", as_index=False)
        .agg(gasto_valor=("valor", "sum"), gasto_litros=("litros", "sum"))
    )

    base = df_limits.copy()
    if "limite_mensal" not in base.columns:
        base["limite_mensal"] = 0.0
    if "limite_litros_mensal" not in base.columns:
        base["limite_litros_mensal"] = 0.0
    base["limite_valor_periodo"] = base["limite_mensal"] * months
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
) -> dict:
    gasto_total = float(df_filtered["valor"].sum())
    gasto_bruto_total = float(df_filtered["valor_bruto"].sum()) if "valor_bruto" in df_filtered.columns else gasto_total
    desconto_total = float(df_filtered["desconto_valor"].sum()) if "desconto_valor" in df_filtered.columns else 0.0
    gasto_litros = float(df_filtered["litros"].sum())

    limite_total_periodo = float(status_df["limite_valor_periodo"].sum()) if "limite_valor_periodo" in status_df.columns else 0.0

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

    color_map = {"GASOLINA": "#2563eb", "DIESEL": "#38bdf8", "ALCOOL": "#f97316"}
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
                text=[f"R$ {v:,.2f}" for v in dados["valor_total"]],
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
        title={"text": "Consumo de combustível por mês e tipo", "x": 0.01, "y": 0.98, "font": {"size": 22}},
        legend={"font": {"size": 16, "color": "#eaf2ff"}, "orientation": "h", "x": 0.01, "y": 1.04, "xanchor": "left", "yanchor": "bottom"},
        xaxis={"tickfont": {"size": 15}},
        yaxis={"tickfont": {"size": 14}},
    )
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
    df = df.sort_values("pct", ascending=True)

    def bar_color(pct):
        if pct > 100:
            return "#ef4444"
        if pct > 80:
            return "#eab308"
        return "#22c55e"

    colors = [bar_color(p) for p in df["pct"]]
    pct_clip = df["pct"].clip(upper=150)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df["secretaria"],
        x=pct_clip,
        orientation="h",
        marker_color=colors,
        text=[
            f"{p:.0f}%  (R$ {v:,.0f} / R$ {l:,.0f})"
            for p, v, l in zip(df["pct"], df["gasto_valor"], df["limite_valor_periodo"])
        ],
        textposition="outside",
        textfont={"size": 12, "color": "#eaf2ff"},
        customdata=list(zip(df["gasto_valor"], df["limite_valor_periodo"], df["pct"])),
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Gasto: R$ %{customdata[0]:,.2f}<br>"
            "Limite: R$ %{customdata[1]:,.2f}<br>"
            "Uso: %{customdata[2]:.1f}%<extra></extra>"
        ),
        showlegend=False,
    ))
    fig.add_vline(x=100, line_dash="dash", line_color="#eab308", line_width=2)
    fig.update_layout(
        template="plotly_dark",
        xaxis_title="% do limite consumido",
        xaxis={"range": [0, 155], "ticksuffix": "%"},
        margin={"l": 20, "r": 80, "t": 60, "b": 30},
        height=max(400, 30 * len(df)),
    )
    fig = apply_plotly_theme(fig)
    fig.update_layout(title={"text": "Gasto em R$ vs. limite por secretaria", "x": 0.01, "y": 0.99, "font": {"size": 20}})
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
        text=[f"{p:.0f}% ({v:,.0f} L / {l:,.0f} L)" for p, v, l in zip(merged["pct"], merged["litros"], merged["limite"])],
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
        margin={"l": 20, "r": 60, "t": 60, "b": 30},
        height=max(400, 28 * len(merged)),
    )
    fig = apply_plotly_theme(fig)
    fig.update_layout(title={"text": "Consumo de litros vs. limite por secretaria", "x": 0.01, "y": 0.99, "font": {"size": 20}})
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

    color_map = {"GASOLINA": "#2563eb", "DIESEL": "#38bdf8", "ALCOOL": "#f97316"}
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
    fig.update_layout(title={"text": "Consumo de combustível por mês e tipo (Litros)", "x": 0.01, "y": 0.98, "font": {"size": 22}})
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
        "GASOLINA": "#38bdf8",
        "DIESEL": "#f97316",
        "DIESEL S10": "#f97316",
        "ALCOOL": "#3b82f6",
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
        margin={"l": 30, "r": 30, "t": 60, "b": 30},
        legend={"font": {"size": 16, "color": "#eaf2ff"}},
    )
    fig = apply_plotly_theme(fig)
    fig.update_layout(title={"text": "Consumo por combustível por mês (Litros)", "x": 0.01, "y": 0.98, "font": {"size": 22}})
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
        empenho_total = float(df_limits["empenho_2026"].sum())
        previsto_total = empenho_total
        previsto_mensal = previsto_total / 12.0

    meses_previstos = [MONTHS[m] for m in range(1, 13)]
    acumulado_previsto = [previsto_mensal * (i + 1) for i in range(12)]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=mensal_real["mes_label"],
            y=mensal_real["acumulado_real"],
            mode="lines+markers+text",
            name="Real acumulado",
            line={"color": "#23b5d3", "width": 3},
            text=[f"{v:,.0f}".replace(",", ".") for v in mensal_real["acumulado_real"]],
            textposition="bottom center",
            textfont={"size": 14},
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
        title={"text": "Gasto acumulado: Real x Previsto", "x": 0.01, "y": 0.98},
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


def apply_discount(df: pd.DataFrame, discount_rate: float) -> pd.DataFrame:
    df = df.copy()
    if "valor" in df.columns:
        df["valor"] = df["valor"] * (1 - discount_rate)
    return df


def apply_filters(
    df: pd.DataFrame,
    selected_ano: str,
    selected_mes: str,
    selected_secretaria: str,
    selected_combustivel: str,
) -> pd.DataFrame:
    df_f = df.copy()
    if selected_ano and selected_ano != "Todos":
        df_f = df_f[df_f["ano"].astype(str) == str(selected_ano)]
    if selected_mes and selected_mes != "Todos":
        mes_num = next((k for k, v in MONTHS.items() if v == selected_mes), None)
        if mes_num is not None:
            df_f = df_f[df_f["mes"] == mes_num]
    if selected_secretaria and selected_secretaria != "Todas":
        df_f = df_f[df_f["secretaria"].str.upper() == selected_secretaria.upper()]
    if selected_combustivel and selected_combustivel != "Todos":
        df_f = df_f[df_f["combustivel"].str.upper() == selected_combustivel.upper()]
    return df_f


def run_dashboard() -> None:
	st.set_page_config(page_title="Painel de Abastecimento", page_icon="⛽", layout="wide", initial_sidebar_state="expanded")
	inject_style()

	df_limits = get_limits_df()
	discount_rate = get_discount_rate()
	df_real = apply_discount(get_real_df("v2_valor_unitario"), discount_rate)

	# Opções dos selectboxes
	secretaria_options = ["Todas"] + sorted(df_limits["secretaria"].dropna().unique().tolist())
	combustivel_options = ["Todos"] + sorted(df_real["combustivel"].dropna().unique().tolist())
	placa_col = next((c for c in ("placa", "veiculo") if c in df_real.columns), None)
	placa_options = (["Todas"] + sorted(df_real[placa_col].dropna().unique().tolist())) if placa_col else []

	anos_disponiveis = ["Todos"] + sorted(df_real["ano"].dropna().unique().astype(str).tolist(), reverse=True)
	meses_disponiveis = ["Todos"] + [MONTHS[m] for m in sorted(MONTHS.keys())]

	# Data máxima para exibir no rodapé
	data_max = df_real["data_hora"].dt.date.max() if "data_hora" in df_real.columns else datetime.date.today()

	with st.sidebar:
		# ── Brand ──
		st.markdown(
			"""<div class="sidebar-brand">
			<span class="sidebar-brand-icon">⛽</span>
			<div>
				<div class="sidebar-brand-title">ABASTECIMENTO</div>
				<div class="sidebar-brand-sub">Frota Municipal</div>
			</div>
			</div>""",
			unsafe_allow_html=True,
		)

		# ── Filtros ──
		st.markdown('<div class="sidebar-section">FILTROS</div>', unsafe_allow_html=True)

		ano_default_idx = next((i for i, a in enumerate(anos_disponiveis) if a == str(datetime.date.today().year)), 0)
		selected_ano = st.selectbox("Ano", anos_disponiveis, index=ano_default_idx, key="sel_ano")
		selected_mes = st.selectbox("Mês", meses_disponiveis, index=0, key="sel_mes")
		selected_secretaria = st.selectbox("Unidade / Secretaria", secretaria_options, index=0, key="sel_sec")
		selected_combustivel = st.selectbox("Produto (Combustível)", combustivel_options, index=0, key="sel_comb")
		if placa_col:
			selected_placa = st.selectbox("Veículo (Placa)", placa_options, index=0, key="sel_placa")
		else:
			selected_placa = None

		if st.button("🗑️ Limpar Filtros", use_container_width=True):
			for k in ("sel_ano", "sel_mes", "sel_sec", "sel_comb", "sel_placa"):
				if k in st.session_state:
					del st.session_state[k]
			st.rerun()

		# ── Alertas (computados com escopo do ano corrente, todas secretarias) ──
		ano_corrente = str(datetime.date.today().year)
		df_alert_base = apply_filters(df_real, ano_corrente, "Todos", "Todas", "Todos")
		status_alerts = build_secretaria_status(df_alert_base, df_limits)

		excedidas = status_alerts[status_alerts["status"] == "ALERTA"].copy() if "status" in status_alerts.columns else pd.DataFrame()
		proximas = status_alerts[
			(status_alerts["desvio_pct"] >= -20) & (status_alerts["desvio_pct"] < 0)
		].copy() if "desvio_pct" in status_alerts.columns else pd.DataFrame()
		ok = status_alerts[status_alerts["status"] == "OK"].copy() if "status" in status_alerts.columns else pd.DataFrame()

		n_exc = len(excedidas)
		n_prox = len(proximas)
		n_ok = len(ok)

		exc_names = ", ".join(excedidas["secretaria"].tolist()[:3]) if n_exc else ""
		prox_names = ", ".join(proximas["secretaria"].tolist()[:3]) if n_prox else ""

		st.markdown('<div class="sidebar-section">ALERTAS</div>', unsafe_allow_html=True)
		st.markdown(
			f"""
			<div class="alert-card alert-red">
				<span class="alert-icon">🔴</span>
				<div class="alert-body">
					<span class="alert-count-red">{n_exc}</span>
					<span class="alert-label">Limite mensal excedido</span>
					<span class="alert-detail">{exc_names}</span>
				</div>
			</div>
			<div class="alert-card alert-yellow">
				<span class="alert-icon">🟡</span>
				<div class="alert-body">
					<span class="alert-count-yellow">{n_prox}</span>
					<span class="alert-label">Próximo do limite (&lt;20%)</span>
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

		# ── Rodapé ──
		ultima_atualizacao = data_max.strftime("%d/%m/%Y") if data_max else "—"
		st.markdown(
			f"""<div class="sidebar-footer">
			🕐 Última atualização: <b>{ultima_atualizacao}</b><br>
			Base: {DB_PATH.name} &nbsp;|&nbsp; Desconto: {discount_rate*100:.2f}%
			</div>""",
			unsafe_allow_html=True,
		)

	# ── Filtrar dados conforme seleção ──
	filtered = apply_filters(df_real, selected_ano, selected_mes, selected_secretaria, selected_combustivel)
	if selected_placa and placa_col and selected_placa != "Todas":
		filtered = filtered[filtered[placa_col] == selected_placa]

	# Para gráfico anual: sem filtro de ano/mês, só secretaria/combustivel
	anual_scope = apply_filters(df_real, "Todos", "Todos", selected_secretaria, selected_combustivel)

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
	ctx_parts = [selected_ano if selected_ano != "Todos" else "Todos os anos"]
	if selected_mes != "Todos":
		ctx_parts.append(selected_mes)
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

	render_kpi_cards(kpis)
	st.caption(
		f"Valores com desconto contratual de {discount_rate * 100:.2f}% aplicado sobre o valor bruto."
	)

	tab_fin, tab_con, tab_sec, tab_vei = st.tabs(["📊 Financeiro", "⛽ Consumo", "🏢 Secretarias", "🚗 Veículos"])

	with tab_fin:
		st.markdown('<p class="section-title">Visão Geral de Gastos</p>', unsafe_allow_html=True)
		col_ano, col_mes = st.columns([1, 2])
		col_ano.plotly_chart(
			make_bar_gasto_por_ano(anual_scope, selected_secretaria, selected_combustivel),
			use_container_width=True, key="bar_gasto_ano",
		)
		col_mes.plotly_chart(
			make_bar_gasto_por_mes_unificado(filtered, selected_secretaria, selected_combustivel),
			use_container_width=True, key="bar_gasto_mes_unificado",
		)
		st.markdown('<p class="section-title">Gasto por Tipo de Combustível e Mês</p>', unsafe_allow_html=True)
		bar_col, donut_col = st.columns([2, 1])
		bar_col.plotly_chart(make_bar_consumo_tipo_mes(filtered), use_container_width=True, key="bar_combustivel_fin")
		donut_col.plotly_chart(make_donut_combustivel_valor(filtered), use_container_width=True, key="donut_combustivel_valor")
		st.markdown('<p class="section-title">Realizado vs Previsto e Projeção</p>', unsafe_allow_html=True)
		st.plotly_chart(
			make_line_real_previsto_projecao(
				filtered,
				limits_scope,
				usar_limite_quinzenal_secretaria=selected_secretaria != "Todas",
			),
			use_container_width=True,
			key="line_real_previsto",
		)
		st.markdown('<p class="section-title">Gasto em R$ vs. Limite por Secretaria</p>', unsafe_allow_html=True)
		st.plotly_chart(
			make_bar_valor_vs_limite_secretaria(status),
			use_container_width=True,
			key="bar_valor_limite_sec",
		)

	with tab_con:
		st.markdown('<p class="section-title">Consumo por Tipo de Combustível e Mês (Litros)</p>', unsafe_allow_html=True)
		bar_con_col, donut_con_col = st.columns([2, 1])
		bar_con_col.plotly_chart(make_bar_consumo_tipo_mes_litros(filtered), use_container_width=True, key="bar_combustivel_litros")
		donut_con_col.plotly_chart(make_donut_combustivel(filtered), use_container_width=True, key="donut_combustivel")
		st.markdown('<p class="section-title">Consumo por Combustível por Mês (Litros)</p>', unsafe_allow_html=True)
		st.plotly_chart(
			make_line_custo_medio_mes_combustivel(filtered),
			use_container_width=True, key="line_custo_medio",
		)
		st.markdown('<p class="section-title">Limite de Litros por Secretaria e Combustível</p>', unsafe_allow_html=True)
		st.plotly_chart(
			make_bar_litros_vs_limite_secretaria(filtered, df_limits),
			use_container_width=True, key="bar_litros_limite_sec",
		)


	with tab_sec:
		st.markdown('<p class="section-title">Ranking de Consumo por Secretaria</p>', unsafe_allow_html=True)
		st.plotly_chart(make_bar_consumo_secretaria(status, df_limits), use_container_width=True, key="bar_sec")
		# Tabela de alertas detalhada
		if not excedidas.empty:
			st.markdown('<p class="section-title">🔴 Secretarias com Limite Excedido</p>', unsafe_allow_html=True)
			cols_show = [c for c in ("secretaria", "gasto_valor", "limite_valor_periodo", "desvio_pct", "desvio_valor") if c in excedidas.columns]
			st.dataframe(excedidas[cols_show].rename(columns={
				"secretaria": "Secretaria",
				"gasto_valor": "Gasto (R$)",
				"limite_valor_periodo": "Limite (R$)",
				"desvio_pct": "Desvio (%)",
				"desvio_valor": "Desvio (R$)",
			}), use_container_width=True)

	with tab_vei:
		st.markdown('<p class="section-title">Análise por Veículo</p>', unsafe_allow_html=True)
		if placa_col:
			top_vei = (
				filtered.groupby(placa_col, as_index=False)
				.agg(total_valor=("valor", "sum"), total_litros=("litros", "sum"), abastecimentos=("valor", "count"))
				.sort_values("total_valor", ascending=False)
				.head(20)
			)
			st.plotly_chart(
				go.Figure(go.Bar(
					y=top_vei[placa_col],
					x=top_vei["total_valor"],
					orientation="h",
					marker_color="#2563eb",
					text=[f"R$ {v:,.0f}" for v in top_vei["total_valor"]],
					textposition="outside",
					textfont={"color": "#fff", "size": 13},
				)).update_layout(
					template="plotly_dark",
					title="Top 20 Veículos por Gasto (R$)",
					yaxis={"categoryorder": "total ascending"},
					margin={"l": 100, "r": 40, "t": 60, "b": 30},
					height=max(400, 35 * len(top_vei)),
				),
				use_container_width=True,
				key="bar_veiculos",
			)
			st.dataframe(
				top_vei.rename(columns={
					placa_col: "Veículo/Placa",
					"total_valor": "Valor Total (R$)",
					"total_litros": "Litros",
					"abastecimentos": "Abastecimentos",
				}),
				use_container_width=True,
			)
		else:
			st.info(
				"⚙️ Esta seção está em desenvolvimento.\n\n"
				"Para ativar, adicione a coluna **`placa`** ou **`veiculo`** na base de dados "
				"importada pelo `convert_relatorio_to_sqlite.py`."
			)

if __name__ == "__main__":
	run_dashboard()

