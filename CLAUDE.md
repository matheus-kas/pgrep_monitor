# CLAUDE.md

Orientações para o Claude Code trabalhar neste repositório.

## O que é o projeto

**Monitor de Replicação PostgreSQL — Rede Auto Shopping.** Dashboard web (Flask)
+ rotinas de notificação que acompanham a replicação entre um PostgreSQL
**master** e sua **réplica**. Calcula lag (tempo e bytes), classifica o estado e
envia **alertas e relatórios por Telegram e e-mail**.

## Arquitetura

- **`app.py`** — Flask: rotas `/api/*`, dashboard e **duas threads daemon**:
  - `monitor_loop`: a cada `check_interval_seconds` consulta a réplica,
    classifica (`_classify`) e notifica **por inscrito** (respeitando preferências).
  - `telegram_polling_loop`: long-polling do bot para auto-inscrição e comandos.
  - As threads sobem via `start_monitor()` (idempotente).
- **`wsgi.py`** — entrada de produção (`gunicorn -w 1 wsgi:app`). Importar `app`
  sozinho **não** inicia as threads; só `wsgi` ou `python app.py` iniciam.
- **`notifier.py`** — envio Telegram (`send_telegram_to`, `send_telegram`,
  `get_updates`) e e-mail (`send_email`); credenciais via `.env`.
- **`subscribers.py`** — inscritos + preferências em `subscribers.json`
  (thread-safe, escrita atômica). `DEFAULT_PREFS` define as prefs padrão.
- **`config.ini`** — seções `[master]`, `[replica]`, `[monitor]` (lido como UTF-8).
- **`sql.py`** — utilitário Tkinter **separado** (consulta de produtos), não faz
  parte do monitor. `diagnostico.py` — diagnóstico de ambiente.

## Regras e armadilhas importantes

- **Uma instância só.** O long-polling do Telegram não admite duas instâncias
  (erro `409 Conflict`) e múltiplos workers duplicam alertas. Sempre `gunicorn -w 1`.
  Em dev, garanta que não há `python app.py` rodando em paralelo (o VSCode às
  vezes sobe uma 2ª com o Python global).
- **Critério de CRÍTICO = atraso em BYTES** (`lag_threshold_bytes`), não em tempo.
  O atraso em tempo cresce sozinho num master ocioso → seria falso alarme.
  Ressalva conhecida: byte-lag não detecta réplica **desconectada** do master.
- **`config.ini` tem acentos** → ler sempre com `encoding="utf-8"`.
- **Console Windows é cp1252**: ao testar prints com emoji, use
  `PYTHONIOENCODING=utf-8`.
- **Segredos não versionados**: `.env` e `subscribers.json` estão no `.gitignore`.
  ⚠️ As senhas de banco no `config.ini` ainda ficam versionadas (pendente migrar p/ `.env`).

## Notificações

- Auto-inscrição: a pessoa envia a senha (`TELEGRAM_SUBSCRIBE_PASSWORD`) ao bot.
- Preferências por inscrito via comandos: `/config`, `/alertas`, `/lembrete`,
  `/relatorio` (off | diario HH:MM | intervalo Nh), `/silencio`. Guia completo em
  `TELEGRAM_SETUP.md`.
- Estados: `OK`, `CRITICO`, `INDISPONIVEL`. Mensagens montadas em
  `_format_report` (texto p/ e-mail, HTML p/ Telegram) + `_titulo`.

## Rodar / testar

```powershell
# dev (Windows)
python app.py                      # http://localhost:5050
# teste de notificação
curl -X POST http://localhost:5050/api/notify/test
# validar sintaxe/lógica
python -m py_compile app.py notifier.py subscribers.py wsgi.py
```

Deploy (Ubuntu 24.04 LTS + Gunicorn + systemd) e firewall: ver `README.md`.

## Convenções

- Código e mensagens em **português**. Manter o estilo existente (sem libs novas
  além de Flask/psycopg2-binary/requests/python-dotenv).
- Commits em português, no padrão do histórico (mensagem descritiva em minúsculas).
- Não commitar nem dar push sem o usuário pedir.
