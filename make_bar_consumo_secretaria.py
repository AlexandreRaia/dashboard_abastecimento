import pandas as pd
import plotly.graph_objects as go
from plotly_utils import apply_plotly_theme


def make_bar_consumo_secretaria(df: pd.DataFrame, df_limits: pd.DataFrame) -> go.Figure:
    # df é o status df: secretaria, gasto_valor, limite_valor_periodo, empenho_2026, ...
    if df is None or df.empty or "secretaria" not in df.columns:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados por secretaria")
        return apply_plotly_theme(fig)
    consumo = df.copy()
    consumo["secretaria"] = consumo["secretaria"].map(lambda x: str(x).strip().upper())

    # Merge empenho_2026 se vier de df_limits separado
    if "empenho_2026" not in consumo.columns and df_limits is not None and "empenho_2026" in df_limits.columns:
        lim = df_limits.copy()
        lim["secretaria"] = lim["secretaria"].map(lambda x: str(x).strip().upper())
        consumo = consumo.merge(lim[["secretaria", "empenho_2026"]], on="secretaria", how="left")

    # --- Melhoria 6: remover secretarias sem gasto ---
    consumo = consumo[consumo["gasto_valor"] > 0].copy()
    if consumo.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="Sem dados de consumo")
        return apply_plotly_theme(fig)

    # --- Melhoria 1: ordenar por % consumido (mais crítico no topo) ---
    consumo["pct"] = consumo.apply(
        lambda r: r["gasto_valor"] / r["limite_valor_periodo"] * 100
        if r.get("limite_valor_periodo", 0) > 0 else 0,
        axis=1,
    )
    consumo = consumo.sort_values("pct", ascending=True)  # ascending=True → mais crítico no topo (plotly inverte)

    # Calcular segmentos das barras
    consumo["consumo_ate_limite"] = consumo.apply(
        lambda r: min(r["gasto_valor"], r.get("limite_valor_periodo", r["gasto_valor"])) if r.get("limite_valor_periodo", 0) > 0 else r["gasto_valor"],
        axis=1,
    )
    consumo["excesso"] = (consumo["gasto_valor"] - consumo.get("limite_valor_periodo", consumo["gasto_valor"])).clip(lower=0) \
        if "limite_valor_periodo" in consumo.columns else pd.Series(0, index=consumo.index)

    if "empenho_2026" in consumo.columns:
        consumo["saldo_empenho"] = (
            consumo["empenho_2026"] - consumo["consumo_ate_limite"] - consumo["excesso"]
        ).clip(lower=0)
    else:
        consumo["saldo_empenho"] = pd.Series(0, index=consumo.index)

    def moeda_br(v):
        return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    # --- Melhoria 3: label com % e R$ ---
    def label_consumo(v, pct):
        if v <= 0:
            return ""
        return f"{pct:.0f}%  ·  {moeda_br(v)}"

    # --- Melhoria 2: cor da barra de consumo por status ---
    def consumo_color(pct):
        if pct > 100:
            return "#ef4444"   # vermelho
        if pct > 80:
            return "#eab308"   # amarelo
        return "#22c55e"       # verde

    bar_colors = [consumo_color(p) for p in consumo["pct"]]

    fig = go.Figure()

    # Barra de consumo (colorida por status)
    fig.add_trace(go.Bar(
        y=consumo["secretaria"],
        x=consumo["consumo_ate_limite"],
        orientation="h",
        name="Consumo até limite",
        marker_color=bar_colors,
        text=[label_consumo(v, p) for v, p in zip(consumo["consumo_ate_limite"], consumo["pct"])],
        textposition="inside",
        insidetextanchor="start",
        textfont={"color": "#fff", "size": 12},
        hovertemplate="<b>%{y}</b><br>Consumo: %{x:,.2f}<extra></extra>",
    ))

    # Barra vermelha: excesso
    fig.add_trace(go.Bar(
        y=consumo["secretaria"],
        x=consumo["excesso"],
        orientation="h",
        name="Excesso sobre limite",
        marker_color="#dc2626",
        text=[moeda_br(v) if v > 0 else "" for v in consumo["excesso"]],
        textposition="inside",
        textfont={"color": "#fff", "size": 11},
        hovertemplate="<b>%{y}</b><br>Excesso: R$ %{x:,.2f}<extra></extra>",
    ))

    # Barra cinza: saldo do empenho
    fig.add_trace(go.Bar(
        y=consumo["secretaria"],
        x=consumo["saldo_empenho"],
        orientation="h",
        name="Saldo do empenho",
        marker_color="#475569",
        marker_opacity=0.6,
        text=["" for _ in consumo["saldo_empenho"]],
        textposition="inside",
        hovertemplate="<b>%{y}</b><br>Saldo: R$ %{x:,.2f}<extra></extra>",
    ))

    # --- Melhoria 2 (legenda atualizada): marcador do limite do período ---
    for sec, lim in zip(consumo["secretaria"], consumo.get("limite_valor_periodo", pd.Series())):
        if pd.notnull(lim) and lim > 0:
            fig.add_shape(
                type="line",
                x0=lim, x1=lim,
                y0=sec, y1=sec,
                line={"color": "#fbbf24", "width": 3, "dash": "dot"},
                xref="x", yref="y", layer="above",
            )

    # Anotação com empenho anual total à direita
    max_x = (consumo["consumo_ate_limite"] + consumo["excesso"] + consumo["saldo_empenho"]).max()
    desl = max_x * 0.012 if max_x > 0 else 1
    if "empenho_2026" in consumo.columns:
        for sec, emp, azul, verm, cinza in zip(
            consumo["secretaria"], consumo["empenho_2026"],
            consumo["consumo_ate_limite"], consumo["excesso"], consumo["saldo_empenho"],
        ):
            fig.add_annotation(
                x=azul + verm + cinza + desl, y=sec,
                text=moeda_br(emp),
                showarrow=False,
                font={"size": 11, "color": "#94a3b8"},
                align="left", xanchor="left", yanchor="middle",
            )

    fig.update_layout(
        barmode="stack",
        template="plotly_dark",
        title="Ranking de Consumo por Secretaria — % do limite e saldo",
        margin={"l": 20, "r": 100, "t": 48, "b": 60},
        bargap=0.25,
        height=max(560, 38 * len(consumo)),
        legend={
            "orientation": "h", "x": 0.5, "y": -0.06,
            "xanchor": "center", "yanchor": "top",
            "font": {"size": 13, "color": "#eaf2ff"},
            "bgcolor": "rgba(0,0,0,0)",
        },
    )
    return apply_plotly_theme(fig)

    # Agrupa por secretaria e soma o valor consumido
    consumo = df.groupby("secretaria", as_index=False)["gasto_valor"].sum()
    # Tenta pegar o limite de cada secretaria, se existir
    if "limite_valor_periodo" in df.columns:
        limites = df.groupby("secretaria", as_index=False)["limite_valor_periodo"].first()
        consumo = consumo.merge(limites, on="secretaria", how="left")
    else:
        consumo["limite_valor_periodo"] = None

    # Normalizar siglas antes do merge para garantir correspondência
    consumo["secretaria"] = consumo["secretaria"].map(lambda x: str(x).strip().upper())
    if df_limits is not None and "empenho_2026" in df_limits.columns:
        df_limits = df_limits.copy()
        df_limits["secretaria"] = df_limits["secretaria"].map(lambda x: str(x).strip().upper())
        consumo = consumo.merge(df_limits[["secretaria", "empenho_2026"]], on="secretaria", how="left")

    consumo = consumo.sort_values("gasto_valor", ascending=False)

    # Calcular barras para garantir proporção correta: azul (gasto), vermelho (excesso), cinza (saldo do empenho)
    if "empenho_2026" in consumo.columns:
        # Azul: gasto até limite, mas nunca maior que o empenho
        consumo["consumo_ate_limite"] = consumo[["gasto_valor", "limite_valor_periodo", "empenho_2026"]].min(axis=1)
        # Vermelho: excesso sobre limite, mas nunca maior que (empenho - consumo_ate_limite)
        consumo["excesso"] = (
            consumo["gasto_valor"] - consumo["limite_valor_periodo"]
        ).clip(lower=0)
        excesso_max = consumo["empenho_2026"] - consumo["consumo_ate_limite"]
        # Corrigir: excesso nunca pode passar do saldo do empenho
        consumo["excesso"] = pd.concat([consumo["excesso"], excesso_max], axis=1).min(axis=1)
        # Cinza: saldo do empenho
        consumo["saldo_empenho"] = consumo["empenho_2026"] - (consumo["consumo_ate_limite"] + consumo["excesso"])
        consumo["saldo_empenho"] = consumo["saldo_empenho"].clip(lower=0)
    else:
        consumo["consumo_ate_limite"] = consumo[["gasto_valor", "limite_valor_periodo"]].min(axis=1)
        consumo["excesso"] = (consumo["gasto_valor"] - consumo["limite_valor_periodo"]).clip(lower=0)
        consumo["saldo_empenho"] = consumo["limite_valor_periodo"] - consumo["gasto_valor"]
        consumo["saldo_empenho"] = consumo["saldo_empenho"].clip(lower=0)

    fig = go.Figure()
    # Barra azul: consumo até limite
    def moeda_br(v):
        return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    fig.add_trace(go.Bar(
        y=consumo["secretaria"],
        x=consumo["consumo_ate_limite"],
        orientation="h",
        name="Consumo até limite",
        marker_color="#2563eb",
        text=[moeda_br(v) if v > 0 else "" for v in consumo["consumo_ate_limite"]],
        textposition="inside",
        textfont={"color": "#fff", "size": 12},
    ))
    # Barra vermelha: excesso (à direita do azul)
    fig.add_trace(go.Bar(
        y=consumo["secretaria"],
        x=consumo["excesso"],
        orientation="h",
        name="Excesso sobre limite",
        marker_color="#e63946",
        text=[moeda_br(v) if v > 0 else "" for v in consumo["excesso"]],
        textposition="inside",
        textfont={"color": "#fff", "size": 12},
    ))
    # Barra cinza: saldo do empenho (empilhada à direita)
    fig.add_trace(go.Bar(
        y=consumo["secretaria"],
        x=consumo["saldo_empenho"],
        orientation="h",
        name="Saldo do empenho",
        marker_color="#94a3b8",
        text=[moeda_br(v) if v > 0 else "" for v in consumo["saldo_empenho"]],
        textposition="inside",
        textfont={"color": "#222", "size": 12},
    ))

    # Limite como linha de referência
    for secretaria, lim in zip(consumo["secretaria"], consumo["limite_valor_periodo"]):
        if pd.notnull(lim) and lim > 0:
            fig.add_shape(
                type="line",
                x0=lim,
                x1=lim,
                y0=secretaria,
                y1=secretaria,
                line={"color": "#eab308", "width": 4, "dash": "dash"},
                xref="x",
                yref="y",
                layer="above"
            )

    # Adiciona o valor total do empenho ao lado direito das barras
    # Ajustar o valor do empenho para aparecer sempre fora da barra (após saldo)
    max_x = (consumo["consumo_ate_limite"] + consumo["excesso"] + consumo["saldo_empenho"]).max()
    deslocamento = max_x * 0.01 if max_x > 0 else 1
    if "empenho_2026" in consumo.columns:
        for idx, (sec, emp, azul, vermelho, cinza) in enumerate(zip(
            consumo["secretaria"], consumo["empenho_2026"], consumo["consumo_ate_limite"], consumo["excesso"], consumo["saldo_empenho"]
        )):
            x_final = azul + vermelho + cinza + deslocamento
            fig.add_annotation(
                x=x_final,
                y=sec,
                text=moeda_br(emp),
                showarrow=False,
                font=dict(size=12, color="#fff"),
                align="left",
                bgcolor="rgba(30,30,30,0.7)",
                bordercolor=None,
                borderwidth=0,
                borderpad=3,
                xanchor="left",
                yanchor="middle",
            )
    else:
        for idx, (sec, azul, vermelho, cinza) in enumerate(zip(
            consumo["secretaria"], consumo["consumo_ate_limite"], consumo["excesso"], consumo["saldo_empenho"]
        )):
            x_final = azul + vermelho + cinza + deslocamento
            fig.add_annotation(
                x=x_final,
                y=sec,
                text=moeda_br(azul + vermelho + cinza),
                showarrow=False,
                font=dict(size=12, color="#fff"),
                align="left",
                bgcolor="rgba(30,30,30,0.7)",
                bordercolor=None,
                borderwidth=0,
                borderpad=3,
                xanchor="left",
                yanchor="middle",
            )

    fig.update_layout(
        barmode="stack",
        template="plotly_dark",
        title="Ranking de Consumo por Secretaria (R$)",
        margin={"l": 120, "r": 40, "t": 48, "b": 30},
        yaxis={"categoryorder": "total ascending"},
        bargap=0.25,
        height=max(600, 40 * len(consumo)),
        legend=dict(
            orientation="h",
            x=0.5,
            y=-0.06,
            xanchor="center",
            yanchor="top",
            font=dict(size=13, color="#eaf2ff"),
            bgcolor="rgba(0,0,0,0)"
        ),
    )
    return apply_plotly_theme(fig)
