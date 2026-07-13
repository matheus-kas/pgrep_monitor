# pgrep_monitor

Dashboard web para monitoramento de replicação PostgreSQL 14 em tempo real. Exibe o status do master, da réplica, lag de replicação, status de conexão streaming e histórico de limpeza de arquivos WAL — com sistema de alertas em três níveis e banner visual para situações críticas.

## Funcionalidades

- **Informações do sistema** — versão, tamanho do banco e hora dos dois servidores
- **Status da réplica** — modo standby/primary, LSN recebido e aplicado, última transação replicada
- **Lag de replicação** — lag em tempo e em bytes, com gráfico histórico da última hora
- **Conexão Master → Réplica** — estado da conexão streaming com tempo de uptime
- **Limpeza de arquivos WAL** — histórico das últimas execuções do `pg_archivecleanup` com tempo desde a última limpeza
- **Sistema de alertas** com três níveis: `ok` / `atenção` / `crítico`
- **Banner de alertas** no topo do dashboard — aparece automaticamente quando algo está fora do limite
- Atualização automática configurável (5s, 15s, 30s ou manual)
- Tema claro e escuro

## Requisitos

- Python 3.10+
- PostgreSQL 14 com replicação streaming configurada
- Usuário `replicator` com permissão de leitura no master e na réplica
- Tabela `archive_cleanup_log` no banco do master (ver seção abaixo)

## Instalação

```bash
git clone <url-do-repo>
cd pgrep_monitor

python -m venv .venv
source .venv/bin/activate  # Linux/macOS

pip install -r requirements.txt
```

## Configuração

### 1. Conexões — `config.ini`

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

# Lag em segundos: atenção / crítico
lag_threshold_seconds = 30
lag_critical_seconds = 300

# Lag em bytes: atenção / crítico
lag_threshold_bytes = 52428800    # 50 MB
lag_critical_bytes = 157286400    # 150 MB

# Minutos sem limpeza WAL: atenção / crítico
cleanup_warn_minutes = 90
cleanup_critical_minutes = 180

report_interval_seconds = 3600
```

> Ajuste os thresholds conforme o tamanho do seu banco e volume de transações.

### 2. Tabela de histórico de limpeza WAL (master)

Execute uma vez no banco do master:

```sql
CREATE TABLE IF NOT EXISTS public.archive_cleanup_log (
    id SERIAL PRIMARY KEY,
    executed_at TIMESTAMPTZ DEFAULT NOW(),
    wal_file VARCHAR(100),
    status VARCHAR(20) NOT NULL DEFAULT 'success',
    message TEXT
);
GRANT SELECT ON public.archive_cleanup_log TO replicator;
```

### 3. Script de limpeza WAL (master)

Salve em `/usr/local/bin/pg_archive_cleanup.sh` e agende no cron do root (`0 * * * *`):

```bash
#!/bin/bash
WAL_FILE=$(sudo -u postgres env PGPASSWORD="<senha>" psql -At -d <banco> -c \
  "SELECT pg_walfile_name(restart_lsn) FROM pg_replication_slots WHERE slot_name='replica_slot_1'")

if [ -n "$WAL_FILE" ]; then
    sudo -u postgres /usr/lib/postgresql/14/bin/pg_archivecleanup /data/pgdata14/archive/ "$WAL_FILE"
    EXIT_CODE=$?
    STATUS=$([ $EXIT_CODE -eq 0 ] && echo "success" || echo "error")
    MSG=$([ $EXIT_CODE -eq 0 ] && echo "Limpeza ate: $WAL_FILE" || echo "pg_archivecleanup falhou (exit $EXIT_CODE)")
else
    STATUS="error"
    MSG="WAL file nao encontrado no slot replica_slot_1"
fi

echo "$(date) - $MSG" >> /var/log/pg_archivecleanup.log

WAL_SQL="NULL"; [ -n "$WAL_FILE" ] && WAL_SQL="'$WAL_FILE'"
MSG_ESC=$(echo "$MSG" | sed "s/'/''/g")

sudo -u postgres env PGPASSWORD="<senha>" PGCLIENTENCODING=UTF8 psql -At -d <banco> -c \
  "INSERT INTO archive_cleanup_log (wal_file, status, message) VALUES ($WAL_SQL, '$STATUS', '$MSG_ESC');
   DELETE FROM archive_cleanup_log WHERE id NOT IN (SELECT id FROM archive_cleanup_log ORDER BY executed_at DESC LIMIT 500);" \
  2>>/var/log/pg_archivecleanup.log
```

### 4. Permissão pg_monitor (recomendado)

Para exibir o estado de conexão da réplica (`streaming`, `async`) com precisão, conceda a role ao usuário no master:

```sql
GRANT pg_monitor TO replicator;
```

Sem essa permissão o dashboard ainda funciona — infere o estado pela presença do cliente na `pg_stat_replication`.

## Execução

```bash
source .venv/bin/activate
python app.py
```

Dashboard disponível em **http://localhost:5050**

## Estrutura do projeto

```
pgrep_monitor/
├── app.py            # Servidor Flask — rotas da API e lógica de alertas
├── config.ini        # Hosts, credenciais e thresholds de alerta
├── requirements.txt  # Dependências Python
├── static/
│   ├── app.js        # Frontend — atualização, cards, banner de alertas
│   └── style.css     # Estilos — tema claro/escuro, badges de status
└── templates/
    └── index.html    # Página principal do dashboard
```

## APIs disponíveis

| Endpoint | Descrição |
|---|---|
| `GET /api/system_info` | Versão, tamanho e hora dos dois servidores |
| `GET /api/replica_mode` | Modo standby/primary e LSNs |
| `GET /api/replica_lag` | Lag em tempo e bytes com nível de alerta |
| `GET /api/replication_status` | Clientes conectados no master |
| `GET /api/replica_lag/history` | Histórico de lag (até 1h, para o gráfico) |
| `GET /api/archive_cleanup` | Histórico de limpeza WAL com nível de alerta |
