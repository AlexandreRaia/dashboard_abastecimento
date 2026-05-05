# KPIs Abastecimento — Painel de Gestão de Frota

Sistema de inteligência analítica para gestão de combustível da Prefeitura de Santana de Parnaíba. Dashboard Streamlit com pipeline multiagente de auditoria.

---

## Sumário

- [Visão Geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Variáveis de Ambiente](#variáveis-de-ambiente)
- [Executar localmente](#executar-localmente)
- [Deploy em servidor](#deploy-em-servidor)
- [Estrutura de diretórios](#estrutura-de-diretórios)
- [Testes](#testes)
- [Importação de dados](#importação-de-dados)
- [Parâmetros financeiros](#parâmetros-financeiros)

---

## Visão Geral

O sistema provê duas aplicações Streamlit:

| Arquivo | Descrição |
|---|---|
| `Abastecimento.py` | Dashboard principal — KPIs, gráficos e análise financeira |
| `pages/Auditoria.py` | Pipeline multiagente de auditoria de irregularidades |

---

## Arquitetura

```
Kpis_abastecimento/
├── config/              ← Configurações centralizadas (settings, constantes)
├── core/
│   ├── services/        ← Lógica de negócio pura (sem I/O)
│   └── utils/           ← Utilitários (formatters, date_utils)
├── infrastructure/
│   ├── api/             ← Cliente HTTP da API Sisatec
│   ├── database/        ← Conexão SQLite e migrações
│   └── repositories/    ← Acesso a dados (abastecimento, parâmetros)
├── ui/
│   ├── components/      ← Componentes reutilizáveis (charts, KPI cards, sidebar)
│   ├── pages/           ← Lógica de cada página Streamlit
│   └── styles/          ← Arquivos CSS externos
├── agents/              ← Pipeline multiagente de auditoria (Agno)
├── assets/              ← Arquivos estáticos (manual HTML)
├── tests/               ← Testes unitários (pytest)
├── Abastecimento.py     ← Entrada Streamlit (thin wrapper)
└── pages/
    ├── Auditoria.py     ← Entrada da página de auditoria
    └── Manutencao.py    ← Entrada da página de manutenção
```

### Princípios de design

- **Separação de camadas**: `config → core → infrastructure → ui`
- **Sem dependências circulares**: camadas inferiores nunca importam camadas superiores
- **Lógica de negócio pura**: `core/services/` não importa Streamlit nem SQLite
- **Ponto único de verdade**: `currency()`, `normalize_secretaria()`, `FUEL_MAP` definidos uma única vez

---

## Pré-requisitos

- Python 3.10+
- pip ou uv
- (Opcional) SQLite CLI para inspeção direta do banco

---

## Instalação

```bash
# Clone o repositório
git clone <repo-url>
cd Kpis_abastecimento

# Crie e ative o ambiente virtual
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Linux/macOS

# Instale as dependências
pip install -r requirements.txt
```

---

## Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto (nunca versionar):

```bash
cp .env.example .env
```

Edite o `.env`:

```env
# Chave da API Sisatec (obrigatória para importação via API)
SISATEC_KEY=sua_chave_aqui
```

A chave pode ser obtida com o fornecedor Sisatec (campo `key` que estava em `config.json` foi removido por segurança).

---

## Executar localmente

```bash
# Dashboard principal
streamlit run Abastecimento.py

# Com porta específica (útil em servidor)
streamlit run Abastecimento.py --server.port 8501 --server.address 0.0.0.0
```

---

## Deploy em servidor

### systemd (Linux)

Crie `/etc/systemd/system/kpis-abastecimento.service`:

```ini
[Unit]
Description=KPIs Abastecimento — Dashboard Streamlit
After=network.target

[Service]
User=www-data
WorkingDirectory=/opt/kpis_abastecimento
EnvironmentFile=/opt/kpis_abastecimento/.env
ExecStart=/opt/kpis_abastecimento/.venv/bin/streamlit run Abastecimento.py \
          --server.port 8501 \
          --server.address 0.0.0.0 \
          --server.headless true
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable kpis-abastecimento
sudo systemctl start kpis-abastecimento
```

### Nginx (proxy reverso)

```nginx
server {
    listen 80;
    server_name dashboard.prefeitura.sp.gov.br;

    location / {
        proxy_pass http://localhost:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}
```

---

## Estrutura de diretórios

```
config/
  settings.py          # Lê .env > config.json para configurações sensíveis
  constants.py         # MONTHS, FUEL_MAP, FUEL_COLORS, YEAR_PALETTE

core/
  services/
    normalization.py   # normalize_secretaria(), normalize_fuel()
    discount_service.py# apply_discount()
    filter_service.py  # apply_filters()
    kpi_service.py     # build_kpis(), build_secretaria_status(), build_alerts()
  utils/
    formatters.py      # currency(), format_litros(), format_percent()
    date_utils.py      # parse_date_series(), date_range_years()

infrastructure/
  api/
    sisatec_client.py  # SisatecClient.fetch(data_inicio, data_fim)
  database/
    connection.py      # get_connection() — WAL + foreign keys
    migrations.py      # ensure_schema() — idempotente
  repositories/
    abastecimento_repo.py  # load_abastecimentos(), import_excel()
    parametros_repo.py     # get_params(), get_all_params(), save_params()

ui/
  components/
    charts.py          # Todas as funções make_*() — Plotly
    kpi_cards.py       # render_kpi_cards()
    sidebar.py         # render_sidebar()
    style_injector.py  # inject_dashboard_style()
  pages/
    dashboard_page.py  # run_dashboard() — orquestrador principal
    auditoria_page.py  # run_auditoria_page()
    manutencao_page.py # run_manutencao_page()
  styles/
    dashboard.css      # CSS do tema escuro

agents/
  orchestrator.py      # OrchestradorAuditoria
  classification.py    # Agente de classificação de anomalias
  ingestion.py         # Agente de ingestão
  validation.py        # Agente de validação
  rules.py             # Regras de negócio de auditoria
  report.py            # Geração de relatórios
  notification.py      # Minutas de notificação
  storage.py           # Persistência de resultados
  history.py           # Histórico de auditorias
  config.py            # THRESHOLDS, COLUMN_MAP

tests/
  test_formatters.py
  test_normalization.py
  test_discount_service.py
  test_kpi_service.py
```

---

## Testes

```bash
# Instale pytest se necessário
pip install pytest

# Executar todos os testes
pytest tests/ -v

# Com relatório de cobertura
pip install pytest-cov
pytest tests/ --cov=core --cov-report=term-missing
```

---

## Importação de dados

### Via upload (recomendado)
Na sidebar do dashboard, use o botão **"Importar relatório Excel"** para carregar o arquivo `Relatorio.xlsx` exportado do sistema Sisatec.

### Via API Sisatec
Na sidebar, expanda **"Atualizar via API Sisatec"** e selecione o período desejado. Requer `SISATEC_KEY` no `.env`.

### Via script de conversão
```bash
python convert_relatorio_to_sqlite.py caminho/para/Relatorio.xlsx
```

---

## Parâmetros financeiros

Os parâmetros de empenho, limites de litros por secretaria e taxa de desconto são configurados diretamente no dashboard:

1. Clique em **"Editar Parâmetros Financeiros"** na sidebar
2. Edite os valores na tabela
3. Clique em **"Salvar"**

Os dados são persistidos no banco `relatorio.db`, tabela `parametros_financeiros_anuais`.

---

## Segurança

- A chave da API Sisatec **nunca deve ser versionada** — use sempre `.env`
- O arquivo `.gitignore` já exclui `.env` e `*.db`
- A conexão SQLite usa WAL mode (leituras concorrentes seguras)
- Entradas do usuário nos filtros são parametrizadas (sem SQL injection)
