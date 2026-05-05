import streamlit as st

from ui.auth import (
    build_authenticator,
    config_needs_save,
    get_allowed_pages,
    load_config,
    save_config,
)

# ── Carrega configuração e cria authenticator ─────────────────────────────
config        = load_config()
authenticator = build_authenticator(config)

# Persiste senhas hasheadas na primeira rodada (auto_hash=True no build)
if config_needs_save(config):
    save_config(config)

# Compartilha com páginas filhas via session_state
st.session_state["authenticator"] = authenticator
st.session_state["auth_config"]   = config

# ── Gate de autenticação ──────────────────────────────────────────────────
if not st.session_state.get("authentication_status"):
    try:
        authenticator.login(
            fields={
                "Form name": "🔐 KPIs Abastecimento — Santana de Parnaíba",
                "Username":  "Usuário",
                "Password":  "Senha",
                "Login":     "Entrar",
            }
        )
    except Exception as exc:
        st.error(exc)

    status = st.session_state.get("authentication_status")
    if status:
        save_config(config)   # persiste logged_in=True
        st.rerun()
    elif status is False:
        st.error("Usuário ou senha incorretos.")
    else:
        st.info("Digite suas credenciais para acessar o sistema.")
    st.stop()

# ── Navegação baseada em perfil ───────────────────────────────────────────
_ALL_PAGES = {
    "Dashboard":  st.Page("pages/Dashboard.py",  title="Dashboard",  icon="📊", default=True),
    "Auditoria":  st.Page("pages/Auditoria.py",  title="Auditoria",  icon="🔍"),
    "Manutenção": st.Page("pages/Manutencao.py", title="Manutenção", icon="🔧"),
    "Lab":        st.Page("pages/lab.py",         title="Lab",        icon="🧪"),
    "Usuários":   st.Page("pages/Usuarios.py",    title="Usuários",   icon="👥"),
}

allowed = get_allowed_pages()
pages   = [_ALL_PAGES[p] for p in allowed if p in _ALL_PAGES]

# ── Sidebar: brand + navegação + usuário + Sair ──────────────────────────
with st.sidebar:
    # Usuário + Sair no topo
    nome = st.session_state.get("name", "")
    st.caption(f"👤 {nome}")
    if st.button("↩ Sair", key="logout_btn"):
        save_config(config)
        authenticator.logout(location="unrendered")
        st.rerun()

    st.divider()

    # Brand
    st.markdown(
        """
        <div style="display:flex;align-items:center;gap:10px;padding:8px 0 16px 0;">
            <span style="font-size:2rem;">⛽</span>
            <div>
                <div style="font-weight:700;font-size:1rem;line-height:1.2;">KPIs Abastecimento</div>
                <div style="font-size:0.75rem;opacity:0.6;">Santana de Parnaíba</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Links de navegação para as páginas permitidas
    for page_name in allowed:
        if page_name in _ALL_PAGES:
            st.page_link(
                _ALL_PAGES[page_name],
                label=page_name,
            )

pg = st.navigation(pages, position="hidden")
pg.run()
