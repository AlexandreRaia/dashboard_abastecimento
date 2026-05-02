"""
Injeção de estilos CSS e scripts JS no dashboard Streamlit.

Carrega o CSS do arquivo externo (ui/styles/dashboard.css) e
injeta fontes + script anti-flash via st.markdown.
"""
from __future__ import annotations

from pathlib import Path

import streamlit as st
import streamlit.components.v1 as st_components


_CSS_PATH = Path(__file__).resolve().parent.parent / "styles" / "dashboard.css"


def inject_dashboard_style() -> None:
    """
    Injeta:
    1. Script JS para suprimir flash branco e definir lang='pt-BR'.
    2. Importação das fontes Rajdhani e Space Grotesk (Google Fonts).
    3. CSS do arquivo ui/styles/dashboard.css.

    Deve ser chamado uma única vez no início de run_dashboard().
    """
    # Script JS: executado antes da renderização do Streamlit
    st_components.html(
        """
        <script>
        (function() {
            var r = window.parent.document.documentElement;
            r.setAttribute('translate', 'no');
            r.setAttribute('lang', 'pt-BR');
            var m = window.parent.document.createElement('meta');
            m.name = 'google'; m.content = 'notranslate';
            window.parent.document.head.appendChild(m);
            // Suprime flash branco imediatamente
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

    # Lê o CSS externo
    css_content = ""
    if _CSS_PATH.exists():
        css_content = _CSS_PATH.read_text(encoding="utf-8")

    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;700&family=Space+Grotesk:wght@400;500;700&display=swap');
        {css_content}
        </style>
        """,
        unsafe_allow_html=True,
    )
