"""
Módulo de autenticação — KPIs Abastecimento.

Usa streamlit-authenticator 0.4.x com credenciais em config/users.yaml.
Senhas em texto plano são hasheadas automaticamente (auto_hash=True).

Perfis disponíveis:
    admin   → todas as páginas
    auditor → Dashboard, Auditoria, Manutenção
    viewer  → Dashboard apenas
"""
from __future__ import annotations

import os
from pathlib import Path

import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

_USERS_FILE = Path(__file__).resolve().parent.parent / "config" / "users.yaml"

# Páginas acessíveis por perfil (ordem importa — primeira é a default)
ROLE_PAGES: dict[str, list[str]] = {
    "admin":   ["Dashboard", "Auditoria", "Manutenção", "Lab", "Usuários"],
    "auditor": ["Dashboard", "Auditoria", "Manutenção"],
    "viewer":  ["Dashboard"],
}

_DEFAULT_CONFIG: dict = {
    "cookie": {
        "expiry_days": 1,
        "key": "",   # preenchido a partir de AUTH_COOKIE_KEY no .env
        "name": "kpis_auth",
    },
    "credentials": {
        "usernames": {
            "admin": {
                "email": "",
                "failed_login_attempts": 0,
                "first_name": "Administrador",
                "last_name": "",
                "logged_in": False,
                "password": "Admin@2026",   # hasheado automaticamente na 1ª rodada
                "roles": ["admin"],
            }
        }
    },
}

# ---------------------------------------------------------------------------
# Funções internas
# ---------------------------------------------------------------------------

def _ensure_users_file() -> None:
    """Cria config/users.yaml com usuário admin padrão se não existir."""
    if _USERS_FILE.exists():
        return
    _USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    cfg = {
        "cookie": {
            "expiry_days": 1,
            "key": os.environ.get("AUTH_COOKIE_KEY", "kpis-cookie-key-mude-em-producao"),
            "name": "kpis_auth",
        },
        "credentials": {
            "usernames": {
                "admin": {
                    "email": "",
                    "failed_login_attempts": 0,
                    "first_name": "Administrador",
                    "last_name": "",
                    "logged_in": False,
                    "password": "Admin@2026",
                    "roles": ["admin"],
                }
            }
        },
    }
    with open(_USERS_FILE, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False)


def _has_plain_text_password(config: dict) -> bool:
    """Retorna True se alguma senha ainda não foi hasheada com bcrypt."""
    for user in config["credentials"]["usernames"].values():
        pw = user.get("password", "")
        if pw and not pw.startswith("$2b$"):
            return True
    return False


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------

def load_config() -> dict:
    """Carrega (e cria se necessário) o arquivo de usuários."""
    _ensure_users_file()
    with open(_USERS_FILE, encoding="utf-8") as f:
        return yaml.load(f, Loader=SafeLoader)


def save_config(config: dict) -> None:
    """Persiste o arquivo de usuários (após login/logout/gestão de usuários)."""
    with open(_USERS_FILE, "w", encoding="utf-8") as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)


def build_authenticator(config: dict) -> stauth.Authenticate:
    """Instancia o Authenticate. auto_hash=True hasheia senhas em plain text."""
    return stauth.Authenticate(
        config["credentials"],
        config["cookie"]["name"],
        config["cookie"]["key"],
        config["cookie"]["expiry_days"],
        auto_hash=True,
    )


def config_needs_save(config: dict) -> bool:
    """
    Verifica se o config precisa ser salvo (ex: senhas ainda em plain text).
    Evita escritas desnecessárias no YAML a cada carregamento de página.
    """
    return _has_plain_text_password(config)


def get_allowed_pages() -> list[str]:
    """Retorna lista de nomes de páginas permitidas para o usuário atual."""
    import streamlit as st
    roles: list[str] = st.session_state.get("roles") or []
    if "admin" in roles:
        return ROLE_PAGES["admin"]
    if "auditor" in roles:
        return ROLE_PAGES["auditor"]
    return ROLE_PAGES["viewer"]
