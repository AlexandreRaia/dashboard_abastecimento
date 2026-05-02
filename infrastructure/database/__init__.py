"""Pacote do sub-sistema de banco de dados."""
from .connection import get_connection
from .migrations import ensure_schema

__all__ = ["get_connection", "ensure_schema"]
