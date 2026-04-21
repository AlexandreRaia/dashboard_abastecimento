def apply_plotly_theme(fig):
    THEME = {
        "panel": "#101926",
        "text": "#eaf2ff"
    }
    fig.update_layout(
        paper_bgcolor=THEME["panel"],
        plot_bgcolor=THEME["panel"],
        font={"color": THEME["text"], "family": "'Space Grotesk', sans-serif", "size": 14},
        title={"font": {"size": 18, "color": THEME["text"]}},
        legend={"font": {"size": 14, "color": "#e8f1ff"}, "bgcolor": "rgba(8, 17, 28, 0.65)"},
        hoverlabel={"bgcolor": "#0b1626", "bordercolor": "#38bdf8", "font": {"color": "#f8fbff", "size": 14}},
    )
    fig.update_xaxes(gridcolor="rgba(142,163,190,0.20)", tickfont={"size": 14})
    fig.update_yaxes(gridcolor="rgba(142,163,190,0.20)", tickfont={"size": 14})
    return fig
