"""
Gerenciamento de configurações da aplicação.

Usa python-dotenv para carregar variáveis de ambiente do arquivo .env.
Todas as configurações sensíveis (chaves de API, credenciais SMTP) devem
estar no arquivo .env e NUNCA devem ser commitadas no controle de versão.

Uso:
    from config.settings import settings
    url = settings.api_base_url
"""
import json
import os
from pathlib import Path

# Tenta carregar .env se python-dotenv estiver disponível
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass  # python-dotenv opcional; variáveis de ambiente do sistema têm precedência


class _Settings:
    """
    Singleton de configurações da aplicação.

    Prioridade de leitura:
        1. Variáveis de ambiente (export SISATEC_KEY=...)
        2. Arquivo .env na raiz do projeto
        3. Valores padrão (sem credenciais sensíveis)
    """

    def __init__(self) -> None:
        self._root = Path(__file__).resolve().parent.parent
        self._cfg = self._load_config_json()

    # ------------------------------------------------------------------
    # Caminhos
    # ------------------------------------------------------------------
    @property
    def root_dir(self) -> Path:
        """Diretório raiz do projeto."""
        return self._root

    @property
    def db_path(self) -> Path:
        """Caminho do banco de dados principal (abastecimentos)."""
        return self._root / os.environ.get("DB_PATH", "relatorio.db")

    # ------------------------------------------------------------------
    # API Sisatec
    # ------------------------------------------------------------------
    @property
    def api_base_url(self) -> str:
        """URL base da API Sisatec."""
        return os.environ.get(
            "SISATEC_BASE_URL",
            self._cfg.get("api_sisatec", {}).get("base_url", ""),
        )

    @property
    def api_codigo(self) -> str:
        """Código do cliente na API Sisatec."""
        return os.environ.get(
            "SISATEC_CODIGO",
            self._cfg.get("api_sisatec", {}).get("codigo", ""),
        )

    @property
    def api_key(self) -> str:
        """
        Chave de autenticação da API Sisatec.

        IMPORTANTE: defina via variável de ambiente SISATEC_KEY no servidor.
        Nunca armazene esta chave no config.json ou no código-fonte.
        """
        key = os.environ.get("SISATEC_KEY", "")
        if not key:
            # Fallback legado: lê do config.json (deve ser removido em produção)
            key = self._cfg.get("api_sisatec", {}).get("key", "")
        return key

    # ------------------------------------------------------------------
    # Parâmetros financeiros padrão
    # ------------------------------------------------------------------
    @property
    def default_discount_rate(self) -> float:
        """Taxa de desconto padrão (0–1). Sobreposta pelos parâmetros anuais no DB."""
        return float(
            os.environ.get(
                "DEFAULT_DISCOUNT_RATE",
                self._cfg.get("desconto_percentual", 0.0405),
            )
        )

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------
    def _load_config_json(self) -> dict:
        """Carrega config.json sem propagar exceção se o arquivo não existir."""
        cfg_path = self._root / "config.json"
        if cfg_path.exists():
            try:
                return json.loads(cfg_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                pass
        return {}


# Instância global — importe `settings` em vez de criar novas instâncias
settings = _Settings()
