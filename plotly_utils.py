CHART_TITLE_FONT = {
    "family": "'Rajdhani', 'Space Grotesk', sans-serif",
    "size": 16,
    "color": "#e7eef8",
}


def apply_plotly_theme(fig):
    THEME = {
        "panel": "#101926",
        "text": "#eaf2ff"
    }
    fig.update_layout(
        paper_bgcolor=THEME["panel"],
        plot_bgcolor=THEME["panel"],
        font={"color": THEME["text"], "family": "'Space Grotesk', sans-serif", "size": 13},
        legend={"font": {"size": 13, "color": "#e8f1ff"}, "bgcolor": "rgba(8, 17, 28, 0.65)"},
        hoverlabel={"bgcolor": "#0b1626", "bordercolor": "#38bdf8", "font": {"color": "#f8fbff", "size": 13}},
        separators=",.",
    )
    if fig.layout.title and fig.layout.title.text:
        fig.update_layout(title={
            "font": CHART_TITLE_FONT,
            "x": 0.01,
            "xanchor": "left",
            "pad": {"t": 8},
        })
    fig.update_xaxes(gridcolor="rgba(142,163,190,0.20)", tickfont={"size": 13})
    fig.update_yaxes(gridcolor="rgba(142,163,190,0.20)", tickfont={"size": 13})
    return fig
