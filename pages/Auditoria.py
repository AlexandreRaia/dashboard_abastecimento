import streamlit as st
from pathlib import Path

# Função de estilo copiada do app.py

def inject_style():
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;700&family=Space+Grotesk:wght@400;500;700&display=swap');
        #MainMenu, footer { display: none !important; }
        header[data-testid="stHeader"] { background: transparent !important; border-bottom: 0 !important; }
        div[data-testid="stToolbar"] { right: 0.75rem; top: 0.35rem; background: transparent !important; }
        button[kind="header"], button[data-testid="collapsedControl"] {
            background: #162436 !important; border: 1px solid rgba(142,163,190,0.35) !important; border-radius: 10px !important; color: #e7eef8 !important; opacity: 1 !important; box-shadow: 0 4px 12px rgba(0, 0, 0, 0.35); }
        button[kind="header"] svg, button[data-testid="collapsedControl"] svg { fill: #e7eef8 !important; }
        [data-testid="collapsedControl"], [data-testid="stSidebarCollapsedControl"] { display: block !important; visibility: visible !important; }
        div[data-testid="stDecoration"] { height: 0 !important; }
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
        section[data-testid="stSidebar"] h1, section[data-testid="stSidebar"] h2, section[data-testid="stSidebar"] h3, section[data-testid="stSidebar"] h4, section[data-testid="stSidebar"] p, section[data-testid="stSidebar"] label, section[data-testid="stSidebar"] .stMarkdown, section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] { color: #dbe7f5 !important; opacity: 1 !important; }
        section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p { color: #c9d8ea !important; font-weight: 600 !important; }
        .block-container { padding-top: 0.35rem; padding-bottom: 1.2rem; max-width: 1600px; }
        </style>
        """,
        unsafe_allow_html=True,
    )

st.set_page_config(page_title="Auditoria", page_icon="🕵️", layout="wide", initial_sidebar_state="expanded")
inject_style()
st.title('Auditoria')
st.write('Conteúdo da página de Auditoria.')
