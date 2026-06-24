# pgrep_monitor

Dashboard web para monitoramento de replicação PostgreSQL em tempo real. Exibe o status do servidor master, da réplica e o lag de replicação (WAL), com histórico gráfico da última hora.

## Funcionalidades

- Status de conexão do master e da réplica
- Lag de replicação em bytes e em tempo
- Gráfico histórico do lag (últimos 60 minutos)
- Atualização automática a cada 5 segundos
- Alertas visuais quando o lag ultrapassa o limite configurado

## Requisitos

- Python 3.10+
- PostgreSQL com replicação streaming configurada
- Usuário com permissão para consultar `pg_stat_replication` no master

## Instalação

```bash
# Clone o repositório
git clone <url-do-repo>
cd pgrep_monitor

# Crie e ative o ambiente virtual
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate  # Linux/macOS

# Instale as dependências
pip install -r requirements.txt
```

## Configuração

### 1. Banco de dados — `config.ini`

```ini
[master]
host = <ip-do-master>
port = 5432
user = replicator
password = <senha>
database = <banco>

[replica]
host = <ip-da-replica>
port = 5432
user = replicator
password = <senha>
database = <banco>

[monitor]
enabled = true
check_interval_seconds = 30
lag_threshold_bytes = 16777216   # 16 MB
lag_threshold_seconds = 30
report_interval_seconds = 3600
```

### 2. Variáveis de ambiente — `.env`

Copie o `.env.example` (se existir) ou crie um `.env` na raiz:

```env
MASTER_DB_PASSWORD=sua_senha
REPLICA_DB_PASSWORD=sua_senha

# Telegram (opcional)
TELEGRAM_ENABLED=false
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
TELEGRAM_SUBSCRIBE_PASSWORD=

# E-mail SMTP (opcional)
EMAIL_ENABLED=false
SMTP_HOST=
SMTP_PORT=587
SMTP_USE_TLS=true
SMTP_USER=
SMTP_PASSWORD=
EMAIL_FROM=
EMAIL_TO=
```

> **Atenção:** o arquivo `.env` contém senhas e **não deve ser versionado**. Ele já está no `.gitignore`.

## Execução

```bash
.venv\Scripts\python.exe app.py
```

O dashboard estará disponível em **http://localhost:5050**.

## Estrutura do projeto

```
pgrep_monitor/
├── app.py            # Servidor Flask e rotas da API
├── notifier.py       # Envio de alertas (Telegram / e-mail)
├── config.ini        # Configuração de hosts e limiares
├── .env              # Senhas e tokens (não versionado)
├── requirements.txt  # Dependências Python
├── static/
│   ├── app.js        # Lógica do frontend
│   └── style.css     # Estilos
└── templates/
    └── index.html    # Página principal
```
