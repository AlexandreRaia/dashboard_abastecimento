from plotly_utils import apply_plotly_theme
import plotly.graph_objects as go
def make_bar_consumo_combustivel(df_filtered):
    if df_filtered.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados por combustível")
        return apply_plotly_theme(fig)

    by_fuel = df_filtered.groupby("combustivel", as_index=False).agg(valor=("valor", "sum"))
    def moeda_br(v):
        return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    fig = go.Figure(go.Bar(
        x=by_fuel["combustivel"],
        y=by_fuel["valor"],
        marker_color=["#1f77b4" if c=="GASOLINA" else ("#ff7f0e" if c=="ALCOOL" else ("#d62728" if c=="DIESEL" else "#38bdf8")) for c in by_fuel["combustivel"]],
        text=[moeda_br(v) for v in by_fuel["valor"]],
        textposition="outside",
        textfont={"size": 20, "color": "#fff", "family": "'Space Grotesk', sans-serif", "weight": "bold"},
        name="Consumo de combustível"
    ))
    fig.update_layout(
        template="plotly_dark",
        title={"text": "Consumo de combustível", "font": {"size": 26, "color": "#f8fbff", "family": "'Space Grotesk', sans-serif", "weight": "bold"}},
        xaxis_title="Tipo de combustível",
        yaxis_title="Valor faturado (R$)",
        margin={"l": 20, "r": 20, "t": 50, "b": 20},
        legend={"font": {"size": 18, "color": "#e8f1ff"}},
    )
    return apply_plotly_theme(fig)
