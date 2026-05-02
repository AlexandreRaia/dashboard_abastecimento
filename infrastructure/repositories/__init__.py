"""Pacote de repositórios de dados."""
from .abastecimento_repo import load_abastecimentos, import_excel
from .parametros_repo import get_params, get_all_params, save_params
from .resolucoes_repo import get_resolucoes, salvar_resolucao, remover_resolucao

__all__ = [
    "load_abastecimentos",
    "import_excel",
    "get_params",
    "get_all_params",
    "save_params",
    "get_resolucoes",
    "salvar_resolucao",
    "remover_resolucao",
]
