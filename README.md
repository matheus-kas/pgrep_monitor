# Monitor de Replicação PostgreSQL — Rede Auto Shopping

Dashboard web + rotinas de notificação para acompanhar a **replicação entre um
servidor PostgreSQL master e sua réplica**. Mostra lag de replicação (em tempo
e em bytes), status da réplica, histórico recente e envia **alertas e
relatórios por Telegram e e-mail**.

---

## Sumário

- [Funcionalidades](#funcionalidades)
- [Como funciona](#como-funciona)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Requisitos](#requisitos)
- [Configuração](#configuração)
  - [config.ini](#configini)
  - [.env (credenciais)](#env-credenciais)
- [Execução local (Windows)](#execução-local-windows)
- [Notificações (Telegram e e-mail)](#notificações-telegram-e-e-mail)
- [Endpoints da API](#endpoints-da-api)
- [Deploy em Ubuntu Server](#deploy-em-ubuntu-server)
- [Segurança](#segurança)
- [Solução de problemas](#solução-de-problemas)

---

## Funcionalidades

- 📊 **Dashboard web** com status do master e da réplica, lag de replicação e histórico (~1h).
- 🔁 **Cálculo de lag** em segundos (tempo desde o último replay) e em bytes (diferença de LSN).
- 🚦 **Classificação de estado**: `OK`, `CRITICO` (lag acima do limite / em recuperação) e `INDISPONIVEL` (sem conexão).
- 🚨 **Alertas em mudança de estado** (sem spam — só avisa quando o estado muda).
- 📅 **Relatório periódico** de status (intervalo configurável).
- 🤖 **Auto-inscrição via bot do Telegram**: a pessoa se inscreve enviando uma senha; não precisa de chat_id fixo.
- ✉️ **E-mail (SMTP)** com STARTTLS ou SSL.

---

## Como funciona

```
┌─────────────────────┐         ┌──────────────────────────────┐
│   Navegador (web)   │ ──────► │  Flask (app.py)  porta 5050   │
└─────────────────────┘         │                               │
                                │  • rotas /api/* (dashboard)   │
┌─────────────────────┐         │  • thread monitor_loop ───────┼──► consulta a réplica
│  Telegram / e-mail  │ ◄────── │  • thread telegram_polling ───┼──► ouve o bot (inscrição)
└─────────────────────┘         └───────────────┬───────────────┘
                                                │
                                  consulta      ▼
                         ┌──────────────┐   ┌──────────────┐
                         │  PostgreSQL  │   │  PostgreSQL  │
                         │   master     │   │   réplica    │
                         └──────────────┘   └──────────────┘
```

Duas threads de background rodam dentro do processo Flask:

1. **`monitor_loop`** — a cada `check_interval_seconds` consulta a réplica,
   classifica o estado e dispara alertas (na mudança) e relatórios (no intervalo).
2. **`telegram_polling_loop`** — escuta o bot do Telegram para processar a
   auto-inscrição por senha (`/start`, senha, `/status`, `/stop`).

> ⚠️ Por causa do long-polling do Telegram, o app deve rodar como **um único
> processo/worker**. Veja [Deploy em Ubuntu Server](#deploy-em-ubuntu-server).

---

## Estrutura do projeto

| Arquivo | Descrição |
|---|---|
| `app.py` | Aplicação Flask: rotas da API, dashboard e threads de monitoramento/bot. |
| `wsgi.py` | Ponto de entrada para produção (Gunicorn). Inicia as threads de background. |
| `notifier.py` | Envio de notificações (Telegram Bot API e SMTP). Lê credenciais do `.env`. |
| `subscribers.py` | Gerência dos inscritos do Telegram em `subscribers.json` (thread-safe). |
| `config.ini` | Conexões master/réplica e parâmetros de monitoramento. |
| `.env` | Credenciais (token do bot, SMTP, senha de inscrição). **Não versionado.** |
| `.env.example` | Modelo do `.env`. |
| `requirements.txt` | Dependências Python. |
| `templates/`, `static/` | Front-end do dashboard. |
| `TELEGRAM_SETUP.md` | Guia para criar o bot e configurar o Telegram. |
| `diagnostico.py` | Script utilitário de diagnóstico do ambiente Python. |
| `sql.py` | Utilitário **separado** (app Tkinter de consulta de produtos) — não faz parte do monitor. |

---

## Requisitos

- **Python 3.10+** (testado em 3.11/3.12).
- Acesso de rede aos PostgreSQL **master** e **réplica**.
- Um usuário no PostgreSQL com permissão para as funções de replicação
  (`pg_is_in_recovery()`, `pg_last_wal_*`, `pg_stat_replication`, etc.).

Dependências Python (em `requirements.txt`):

```
Flask
psycopg2-binary
requests
python-dotenv
```

---

## Configuração

### config.ini

Conexões e parâmetros de monitoramento (este arquivo **é versionado** — veja a
ressalva em [Segurança](#segurança) sobre as senhas de banco):

```ini
[master]
host = 10.8.0.235
port = 5432
user = replicator
password = ********
database = postgres

[replica]
host = 10.8.0.3
port = 5432
user = postgres
password = ********
database = postgres

[monitor]
# Liga/desliga a thread de monitoramento em background
enabled = true
# Intervalo entre verificações de lag (segundos)
check_interval_seconds = 30
# Lag acima deste valor (segundos) é considerado crítico
lag_threshold_seconds = 30
# Intervalo do relatório periódico de status (segundos). Ex.: 3600 = 1h
report_interval_seconds = 3600
```

### .env (credenciais)

Copie `.env.example` para `.env` e preencha. **Nunca** versione o `.env`
(já está no `.gitignore`).

```ini
# ---------- Telegram ----------
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=123456789:AA...token...
# Opcional: chat fixo (admin). Pode deixar vazio — a auto-inscrição resolve.
TELEGRAM_CHAT_ID=
# Senha que a pessoa envia ao bot para se inscrever
TELEGRAM_SUBSCRIBE_PASSWORD=umaSenhaForte

# ---------- E-mail (SMTP) ----------
EMAIL_ENABLED=false
SMTP_HOST=smtp.exemplo.com.br
SMTP_PORT=587
SMTP_USE_TLS=true
SMTP_USER=no-reply@exemplo.com.br
SMTP_PASSWORD=
EMAIL_FROM=no-reply@exemplo.com.br
# Vários destinatários separados por vírgula
EMAIL_TO=fulano@exemplo.com.br, ciclano@exemplo.com.br
```

> Use porta **587** com `SMTP_USE_TLS=true` (STARTTLS) ou porta **465** (SSL,
> detectado automaticamente).

---

## Execução local (Windows)

```powershell
# 1. (recomendado) crie e ative um ambiente virtual
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2. instale as dependências
pip install -r requirements.txt

# 3. configure config.ini e .env (veja acima)

# 4. rode
python app.py
```

A aplicação sobe em **http://localhost:5050** e no terminal você verá:

```
[monitor] thread de monitoramento iniciada
[bot] polling de auto-inscrição iniciado
```

---

## Notificações (Telegram e e-mail)

O passo a passo completo para **criar o bot, obter o token e configurar** está
em **[TELEGRAM_SETUP.md](TELEGRAM_SETUP.md)**.

**Resumo da auto-inscrição:** com `TELEGRAM_SUBSCRIBE_PASSWORD` definido, cada
pessoa abre o bot → `/start` → envia a senha → passa a receber os status. O
`chat_id` é guardado em `subscribers.json`.

Comandos do bot:

| Comando | Ação |
|---------|------|
| `/start` ou `/ajuda` | Instruções |
| *(enviar a senha)* | Inscreve o usuário |
| `/status` | Status atual da réplica (só inscritos) |
| `/config` | Mostra/edita preferências de notificação |
| `/alertas on\|off` | Alertas de mudança de estado |
| `/lembrete on [min] \| off` | Re-lembrete enquanto crítico |
| `/relatorio off \| diario HH:MM \| intervalo Nh` | Relatório periódico |
| `/silencio HH:MM HH:MM \| off` | Horário silencioso |
| `/stop` | Cancela a inscrição |

Cada inscrito controla as próprias preferências (guardadas em `subscribers.json`).
O critério de **CRÍTICO** é o atraso em **bytes** (`lag_threshold_bytes` no
`config.ini`) — o atraso em tempo é apenas informativo, pois cresce sozinho num
master ocioso.

**Teste manual** (com o app rodando):

```bash
curl -X POST http://localhost:5050/api/notify/test
```

---

## Endpoints da API

| Método | Rota | Descrição |
|--------|------|-----------|
| `GET` | `/` | Dashboard web. |
| `GET` | `/api/system_info` | Versão, tamanho do banco, IP e horário do master e da réplica. |
| `GET` | `/api/replica_mode` | Modo standby e LSNs (receive/replay/current). |
| `GET` | `/api/replica_lag` | Lag atual da réplica (segundos e bytes) + status. |
| `GET` | `/api/replica_lag/history?limit=200` | Histórico recente de lag (memória, ~1h). |
| `GET` | `/api/replication_status` | Clientes de replicação ativos no master (`pg_stat_replication`). |
| `GET`/`POST` | `/api/notify/test` | Dispara uma notificação de teste nos canais habilitados. |

---

## Deploy em Ubuntu Server

### Qual versão usar?

**Recomendado: Ubuntu Server 24.04 LTS (Noble Numbat).**

| Versão | Situação | Suporte | Observação |
|--------|----------|---------|------------|
| 22.04 LTS | Madura | até 2027 (ESM 2032) | Python 3.10. Boa, mas mais antiga. |
| **24.04 LTS** | **Madura e estável** | **até 2029 (ESM 2034)** | **Python 3.12. Melhor equilíbrio — escolha esta.** |
| 26.04 LTS | Recém-lançada (abr/2026) | até 2031+ | Python mais novo; por ser `.0` recente, prefira aguardar o `.1`. |

**Por que a 24.04 LTS:** é LTS (5 anos de suporte), madura, com Python 3.12 e
amplamente documentada. A 26.04, embora tenha a maior validade, acabou de sair
e ainda pode ter ajustes iniciais — para produção, a 24.04 é a aposta segura.
Evite versões **não-LTS** (ex.: 24.10, 25.04) em servidor: têm só 9 meses de suporte.

### 1. Pacotes do sistema

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-venv python3-pip git
```

### 2. Obter o código

```bash
sudo mkdir -p /opt/pgrep_monitor
sudo chown $USER:$USER /opt/pgrep_monitor
git clone <URL_DO_SEU_REPO> /opt/pgrep_monitor
cd /opt/pgrep_monitor
```

### 3. Ambiente virtual + dependências (inclui Gunicorn)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt gunicorn
```

### 4. Configurar `config.ini` e `.env`

```bash
cp .env.example .env
nano .env          # preencha token do bot, senha de inscrição e SMTP
nano config.ini    # ajuste master/réplica e a seção [monitor]
chmod 600 .env     # restringe a leitura das credenciais
```

### 5. Teste rápido (manual)

```bash
gunicorn -w 1 -b 0.0.0.0:5050 wsgi:app
# Ctrl+C para parar após confirmar que sobe sem erros
```

> ⚠️ **Use sempre `-w 1` (um worker).** O long-polling do Telegram não admite
> duas instâncias (retorna erro `409 Conflict`) e múltiplos workers
> duplicariam os alertas. Para mais conexões simultâneas no dashboard, use
> threads em vez de workers: `-w 1 --threads 4`.

### 6. Serviço systemd (inicialização automática)

Crie `/etc/systemd/system/pgrep-monitor.service`:

```ini
[Unit]
Description=Monitor de Replicacao PostgreSQL
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=www-data
Group=www-data
WorkingDirectory=/opt/pgrep_monitor
ExecStart=/opt/pgrep_monitor/.venv/bin/gunicorn -w 1 --threads 4 -b 0.0.0.0:5050 wsgi:app
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

> O `User`/`Group` (`www-data`) precisa ter permissão de leitura no projeto e
> no `.env`. O passo 6 ajusta o dono dos arquivos com `chown -R www-data:www-data`.

Ative e inicie:

```bash
sudo chown -R www-data:www-data /opt/pgrep_monitor
sudo systemctl daemon-reload
sudo systemctl enable --now pgrep-monitor
sudo systemctl status pgrep-monitor
journalctl -u pgrep-monitor -f      # acompanha os logs em tempo real
```

### 7. Firewall

```bash
sudo ufw allow 5050/tcp
```

### 8. (Opcional) Nginx como proxy reverso

Para servir na porta 80/443 com domínio e HTTPS:

```nginx
server {
    listen 80;
    server_name monitor.suaempresa.com.br;

    location / {
        proxy_pass http://127.0.0.1:5050;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

```bash
sudo apt install -y nginx
# salve o bloco acima em /etc/nginx/sites-available/pgrep-monitor e crie o symlink
sudo ln -s /etc/nginx/sites-available/pgrep-monitor /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
# HTTPS gratuito com Certbot (opcional):
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d monitor.suaempresa.com.br
```

Com Nginx na frente, você pode fechar a porta 5050 no firewall e expor só 80/443.

### Atualizar o app no servidor

```bash
cd /opt/pgrep_monitor
git pull
source .venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart pgrep-monitor
```

---

## Segurança

- **Nunca** versione o `.env` nem o `subscribers.json` (já estão no `.gitignore`).
- Use `chmod 600 .env` no servidor.
- A senha de inscrição (`TELEGRAM_SUBSCRIBE_PASSWORD`) é a única barreira para
  receber notificações — use uma senha forte e troque-a se vazar (atualize o
  `.env` e reinicie o serviço).
- Se o **token do bot** vazar, gere outro no @BotFather (`/revoke`) e atualize o `.env`.
- ⚠️ **As senhas de banco no `config.ini` ficam versionadas.** Recomenda-se
  movê-las para o `.env` futuramente. Se já foram commitadas, considere
  trocá-las no PostgreSQL.
- O dashboard **não tem autenticação** — exponha-o apenas na rede interna/VPN
  ou atrás de um proxy com autenticação.

---

## Solução de problemas

| Sintoma | Causa provável | Solução |
|---------|----------------|---------|
| Não chega notificação no Telegram | Token/senha errados ou pessoa não inscrita | Veja `journalctl -u pgrep-monitor -f`; teste `/api/notify/test`. |
| `409 Conflict` no log do bot | Mais de um processo fazendo polling | Garanta **1 worker** (`-w 1`) e apenas uma instância rodando. |
| E-mail não envia | `EMAIL_ENABLED=false` ou SMTP errado | Ative e confira host/porta/senha; veja o detalhe no log. |
| `error` nas rotas `/api/*` | Sem acesso ao PostgreSQL | Confira `config.ini`, rede/firewall e permissões do usuário. |
| Threads não sobem com Gunicorn | App iniciado via `app:app` | Use **`wsgi:app`** (inicia as threads de background). |

---

Projeto interno — **Rede Auto Shopping**.
