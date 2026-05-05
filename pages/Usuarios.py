"""
Página de gerenciamento de usuários — acesso exclusivo para admins.
"""
import streamlit as st
import bcrypt
import pandas as pd

from ui.auth import load_config, save_config, ROLE_PAGES

_ROLES_DISPONIVEIS = ["admin", "auditor", "viewer"]
_ROLE_LABELS = {
    "admin":   "👑 Admin",
    "auditor": "🔍 Auditor",
    "viewer":  "👁️ Visualizador",
}


def _check_admin_access() -> None:
    roles: list = st.session_state.get("roles") or []
    if "admin" not in roles:
        st.error("🚫 Acesso negado. Esta página é exclusiva para administradores.")
        st.stop()


def _hash(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def _render_user_table(users: dict) -> None:
    rows = []
    for uname, data in users.items():
        nome = f"{data.get('first_name', '')} {data.get('last_name', '')}".strip()
        perfis = [_ROLE_LABELS.get(r, r) for r in (data.get("roles") or [])]
        rows.append({
            "Usuário": uname,
            "Nome": nome,
            "E-mail": data.get("email", ""),
            "Perfis": ", ".join(perfis),
        })
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum usuário cadastrado.")


def _render_edit_panel(config: dict, users: dict) -> None:
    current_user = st.session_state.get("username", "")
    selected = st.selectbox("Selecionar usuário para editar", list(users.keys()))
    if not selected:
        return

    user = users[selected]

    col_role, col_pass = st.columns(2)

    with col_role:
        with st.expander("🔑 Alterar perfis"):
            current_roles = user.get("roles") or ["viewer"]
            new_roles = st.multiselect(
                "Perfis",
                _ROLES_DISPONIVEIS,
                default=current_roles,
                format_func=lambda r: _ROLE_LABELS.get(r, r),
                key=f"roles_{selected}",
            )
            if st.button("Salvar perfis", key=f"save_roles_{selected}"):
                if not new_roles:
                    st.error("Selecione ao menos um perfil.")
                else:
                    users[selected]["roles"] = new_roles
                    save_config(config)
                    st.success("Perfis atualizados!")
                    st.rerun()

    with col_pass:
        with st.expander("🔐 Resetar senha"):
            new_pass = st.text_input(
                "Nova senha (mín. 8 caracteres)",
                type="password",
                key=f"pass_{selected}",
            )
            if st.button("Salvar senha", key=f"reset_{selected}"):
                if len(new_pass) < 8:
                    st.error("Senha deve ter ao menos 8 caracteres.")
                else:
                    users[selected]["password"] = _hash(new_pass)
                    save_config(config)
                    st.success("Senha alterada com sucesso!")

    st.divider()

    is_last_admin = (
        "admin" in (user.get("roles") or [])
        and sum(1 for u in users.values() if "admin" in (u.get("roles") or [])) <= 1
    )

    if selected == current_user:
        st.caption("ℹ️ Você não pode excluir sua própria conta.")
    elif is_last_admin:
        st.caption("⚠️ Não é possível excluir o único administrador do sistema.")
    else:
        if st.button(
            f"🗑️ Excluir '{selected}'",
            type="primary",
            key=f"del_{selected}",
        ):
            del users[selected]
            save_config(config)
            st.success(f"Usuário '{selected}' excluído.")
            st.rerun()


def _render_add_user(config: dict) -> None:
    users = config["credentials"]["usernames"]

    with st.form("form_add_user", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            first_name = st.text_input("Nome *")
            username   = st.text_input("Usuário (login) *")
            password   = st.text_input("Senha * (mín. 8 caracteres)", type="password")
        with col2:
            last_name = st.text_input("Sobrenome")
            email     = st.text_input("E-mail")
            roles     = st.multiselect(
                "Perfis *",
                _ROLES_DISPONIVEIS,
                default=["viewer"],
                format_func=lambda r: _ROLE_LABELS.get(r, r),
            )
        submitted = st.form_submit_button("➕ Adicionar usuário", use_container_width=True)

    if not submitted:
        return

    errors: list[str] = []
    if not first_name:
        errors.append("Nome é obrigatório.")
    if not username:
        errors.append("Usuário é obrigatório.")
    elif username in users:
        errors.append(f"Usuário '{username}' já existe.")
    if len(password) < 8:
        errors.append("Senha deve ter ao menos 8 caracteres.")
    if not roles:
        errors.append("Selecione ao menos um perfil.")

    if errors:
        for err in errors:
            st.error(err)
        return

    users[username] = {
        "email": email,
        "failed_login_attempts": 0,
        "first_name": first_name,
        "last_name": last_name,
        "logged_in": False,
        "password": _hash(password),
        "roles": roles,
    }
    save_config(config)
    st.success(f"✅ Usuário '{username}' adicionado com sucesso!")
    st.rerun()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

_check_admin_access()

st.title("👥 Gerenciamento de Usuários")

config = load_config()
users  = config["credentials"]["usernames"]

tab_lista, tab_novo = st.tabs(["📋 Usuários cadastrados", "➕ Novo usuário"])

with tab_lista:
    _render_user_table(users)
    st.divider()
    st.subheader("Editar / excluir")
    _render_edit_panel(config, users)

with tab_novo:
    _render_add_user(config)
