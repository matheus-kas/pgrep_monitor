#!/usr/bin/env python3
from flask import Flask, jsonify, render_template, send_from_directory, request
from threading import Lock, Thread
import collections
import configparser
import psycopg2
import psycopg2.extras
import time
from datetime import datetime, timezone

import notifier

app = Flask(__name__, template_folder="templates", static_folder="static")

# Read config.ini from same folder
cfg = configparser.ConfigParser()
cfg.read("config.ini")

# Simple in-memory cache to avoid hammering DB every 5s
_cache = {}
CACHE_TTL = 2.0  # seconds

# History buffer for replica lag (kept in memory)
HISTORY_REPLICA_LAG = collections.deque()
HISTORY_LOCK = Lock()
HISTORY_MAX_SAMPLES = 720  # keep up to ~1h @ 5s intervals (720 * 5s = 3600s)
HISTORY_RETENTION_SECONDS = 3600  # prune samples older than 1 hour (seconds)

def get_conn(section):
    if section not in cfg:
        raise RuntimeError(f"Seção {section} não encontrada no config.ini")
    s = cfg[section]
    conn = psycopg2.connect(
        host=s.get("host"),
        port=s.get("port", 5432),
        user=s.get("user"),
        password=s.get("password"),
        dbname=s.get("database", "postgres"),
        connect_timeout=5
    )
    return conn

def cached(key, fn):
    now = time.time()
    entry = _cache.get(key)
    if entry and (now - entry["t"] < CACHE_TTL):
        return entry["v"]
    v = fn()
    _cache[key] = {"v": v, "t": now}
    return v

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/system_info")
def api_system_info():
    def fetch():
        out = {"master": {}, "replica": {}}
        for section in ("master", "replica"):
            try:
                conn = get_conn(section)
                cur = conn.cursor()
                cur.execute("SELECT version();")
                out[section]["pg_version"] = cur.fetchone()[0]
                cur.execute("SELECT pg_database_size(current_database())")
                size = cur.fetchone()[0]
                cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
                out[section]["db_size_pretty"] = cur.fetchone()[0]
                out[section]["db_size_bytes"] = size
                cur.execute("SELECT current_timestamp")
                out[section]["server_time"] = cur.fetchone()[0].isoformat()
                cur.execute("SELECT inet_server_addr()")
                ip = cur.fetchone()[0]
                out[section]["server_ip"] = ip
                # is in recovery (replica true)
                cur.execute("SELECT pg_is_in_recovery()")
                out[section]["in_recovery"] = cur.fetchone()[0]
                cur.close()
                conn.close()
            except Exception as e:
                out[section]["error"] = str(e)
        return out
    return jsonify(cached("system_info", fetch))

@app.route("/api/replica_mode")
def api_replica_mode():
    def fetch():
        try:
            conn = get_conn("replica")
            cur = conn.cursor()

            # Verifica se está em modo recovery
            cur.execute("SELECT pg_is_in_recovery();")
            in_recovery = cur.fetchone()[0]

            receive_lsn = replay_lsn = current_lsn = last_xact_replay_ts = None

            # Sempre pode buscar esses (válidos na réplica)
            cur.execute("""
                SELECT 
                    pg_last_wal_receive_lsn(),
                    pg_last_wal_replay_lsn(),
                    pg_last_xact_replay_timestamp()
            """)
            receive_lsn, replay_lsn, last_xact_replay_ts = cur.fetchone()

            # Só tenta pegar o current_lsn se NÃO estiver em recovery
            if not in_recovery:
                cur.execute("SELECT pg_current_wal_lsn();")
                current_lsn = cur.fetchone()[0]

            cur.close()
            conn.close()

            # Tenta também buscar o current_lsn do MASTER (útil quando réplica está em standby)
            master_current_lsn = None
            master_server_time = None
            try:
                mconn = get_conn('master')
                mcur = mconn.cursor()
                mcur.execute('SELECT pg_current_wal_lsn(), current_timestamp')
                row = mcur.fetchone()
                master_current_lsn = row[0]
                master_server_time = row[1].isoformat() if row[1] else None
                mcur.close()
                mconn.close()
            except Exception as e:
                # Log leve para ajudar debugging em ambiente local
                print(f"[debug] não foi possível obter current_lsn do master: {e}")

            return {
                "is_standby": bool(in_recovery),
                "current_lsn": current_lsn,
                "receive_lsn": receive_lsn,
                "replay_lsn": replay_lsn,
                "last_replay_time": last_xact_replay_ts.isoformat() if last_xact_replay_ts else None,
                "master_current_lsn": master_current_lsn,
                "master_server_time": master_server_time
            }
        except Exception as e:
            return {"error": str(e)}
    return jsonify(cached("replica_mode", fetch))

def _record_replica_lag_history(result):
    """Adiciona uma amostra ao histórico em memória (thread-safe)."""
    try:
        now_utc = datetime.now(timezone.utc)
        sample = {
            "ts": now_utc.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "replay_lag_seconds": result.get("replay_lag_seconds"),
            "exact_byte_lag": result.get("exact_byte_lag")
        }
        with HISTORY_LOCK:
            HISTORY_REPLICA_LAG.append(sample)
            # trim by count
            while len(HISTORY_REPLICA_LAG) > HISTORY_MAX_SAMPLES:
                HISTORY_REPLICA_LAG.popleft()
            # prune by age
            cutoff = datetime.now(timezone.utc).timestamp() - HISTORY_RETENTION_SECONDS
            while HISTORY_REPLICA_LAG and (datetime.fromisoformat(HISTORY_REPLICA_LAG[0]["ts"].replace('Z','')).timestamp() < cutoff):
                HISTORY_REPLICA_LAG.popleft()
    except Exception:
        # não deixa a manutenção do histórico derrubar o chamador
        pass


def compute_replica_lag():
    """Consulta a réplica e retorna o estado de lag (sem efeitos colaterais).

    Em caso de falha de conexão/consulta, retorna {"error": "..."}.
    Reutilizado pela API e pela thread de monitoramento.
    """
    try:
        conn = get_conn("replica")
        cur = conn.cursor()
        # Basic values
        cur.execute("SELECT pg_is_in_recovery()")
        in_recovery = cur.fetchone()[0]

        # times and LSNs
        cur.execute("""
            SELECT
                pg_last_xact_replay_timestamp() AS last_replay_ts,
                EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())) AS replay_lag_seconds,
                pg_last_wal_receive_lsn() AS receive_lsn,
                pg_last_wal_replay_lsn() AS replay_lsn,
                CASE
                  WHEN pg_last_wal_receive_lsn() IS NULL OR pg_last_wal_replay_lsn() IS NULL THEN NULL
                  ELSE pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())::bigint
                END AS exact_byte_lag
            """)
        row = cur.fetchone()
        last_replay_ts, replay_lag_seconds, receive_lsn, replay_lsn, exact_byte_lag = row

        # Debug logging when values are unexpectedly null to help troubleshooting
        if replay_lag_seconds is None:
            print(f"[debug] replay_lag_seconds is None (last_replay_ts={last_replay_ts}, receive_lsn={receive_lsn}, replay_lsn={replay_lsn})")
        if receive_lsn is None or replay_lsn is None:
            print(f"[debug] receive_lsn or replay_lsn is None (receive={receive_lsn}, replay={replay_lsn})")

        cur.close()
        conn.close()

        # Pretty sizes
        def pretty_bytes(b):
            if b is None:
                return None
            b = float(b)
            for unit in ['B','KB','MB','GB','TB']:
                if b < 1024:
                    return f"{b:.1f} {unit}"
                b /= 1024.0
            return f"{b:.1f} PB"

        lag_pretty = pretty_bytes(exact_byte_lag) if exact_byte_lag is not None else None

        # Decide status: in_recovery + seconds > threshold -> recovering
        threshold = 5.0
        status = "PRONTO"
        if in_recovery:
            if replay_lag_seconds is None:
                status = "EM RECUPERAÇÃO"
            elif replay_lag_seconds > threshold:
                status = "EM RECUPERAÇÃO"
            else:
                status = "PRONTO"

        result = {
            "in_recovery": in_recovery,
            "last_replay_timestamp": last_replay_ts.isoformat() if last_replay_ts else None,
            "replay_lag_seconds": float(replay_lag_seconds) if replay_lag_seconds is not None else None,
            "replay_lag_seconds_rounded": round(replay_lag_seconds, 1) if replay_lag_seconds is not None else None,
            "receive_lsn": receive_lsn,
            "replay_lsn": replay_lsn,
            "exact_byte_lag": int(exact_byte_lag) if exact_byte_lag is not None else None,
            "lag_pretty": lag_pretty,
            "status": status
        }
        return result
    except Exception as e:
        return {"error": str(e)}


@app.route("/api/replica_lag")
def api_replica_lag():
    def fetch():
        result = compute_replica_lag()
        if "error" not in result:
            _record_replica_lag_history(result)
        return result
    return jsonify(cached("replica_lag", fetch))

@app.route("/api/replication_status")
def api_replication_status():
    def fetch():
        try:
            conn = get_conn("master")
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            # Query active replication clients on master
            cur.execute("""
                SELECT application_name, client_addr, state, sync_state,
                  write_lag, flush_lag, replay_lag, backend_start,
                  now() - backend_start AS connection_duration
                FROM pg_stat_replication
            """)
            rows = cur.fetchall()
            if not rows:
                print('[debug] nenhum cliente de replicação ativo em master (pg_stat_replication)')
            clients = []
            for r in rows:
                conn_dur = r['connection_duration'].total_seconds() if r['connection_duration'] is not None else None
                clients.append({
                    "application_name": r['application_name'],
                    "client_addr": str(r['client_addr']),
                    "state": r['state'],
                    "sync_state": r['sync_state'] if r['sync_state'] is not None else None,
                    "write_lag": (str(r['write_lag']) if r['write_lag'] is not None else None),
                    "flush_lag": (str(r['flush_lag']) if r['flush_lag'] is not None else None),
                    "replay_lag": (str(r['replay_lag']) if r['replay_lag'] is not None else None),
                    "backend_start": r['backend_start'].isoformat() if r['backend_start'] else None,
                    "connection_duration_seconds": conn_dur
                })
            return clients
        except Exception as e:
            return {"error": str(e)}
    return jsonify(cached("replication_status", fetch))


@app.route('/api/replica_lag/history')
def api_replica_lag_history():
    """Retorna histórico de lag da réplica (mais recente por padrão).
    Query params:
      - limit: máximo de amostras (default 200)
    """
    try:
        limit = int(request.args.get('limit', 200))
        with HISTORY_LOCK:
            # return as list (oldest -> newest)
            items = list(HISTORY_REPLICA_LAG)[-limit:]
        return jsonify(items)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/static/<path:path>")
def static_files(path):
    return send_from_directory("static", path)


# ---------------------------------------------------------------------------
# Monitoramento em background + notificações (Telegram / e-mail)
# ---------------------------------------------------------------------------

def _classify(result):
    """Reduz o resultado de compute_replica_lag a um estado simples.

    Retorna ("OK"|"CRITICO"|"INDISPONIVEL", mensagem_humana).
    """
    if "error" in result:
        return "INDISPONIVEL", f"Falha ao consultar a réplica: {result['error']}"

    m = cfg["monitor"] if "monitor" in cfg else {}
    threshold = float(m.get("lag_threshold_seconds", 30))
    lag = result.get("replay_lag_seconds")
    status = result.get("status")

    if status == "EM RECUPERAÇÃO" or (lag is not None and lag > threshold):
        lag_txt = f"{lag:.1f}s" if lag is not None else "desconhecido"
        return "CRITICO", f"Lag de replicação alto: {lag_txt} (status={status})."
    return "OK", f"Réplica sincronizada (lag={result.get('replay_lag_seconds_rounded')}s, status={status})."


def _format_report(result, estado, msg):
    """Monta texto legível com os números atuais para relatório/alerta."""
    linhas = [
        msg,
        "",
        f"Estado: {estado}",
        f"Lag (tempo): {result.get('replay_lag_seconds_rounded')} s",
        f"Lag (bytes): {result.get('lag_pretty')}",
        f"Em recuperação: {result.get('in_recovery')}",
        f"Último replay: {result.get('last_replay_timestamp')}",
        f"Horário (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}",
    ]
    return "\n".join(str(l) for l in linhas)


def monitor_loop():
    """Verifica o lag periodicamente, dispara alertas em mudança de estado e
    envia relatórios periódicos. Roda em uma thread daemon."""
    m = cfg["monitor"] if "monitor" in cfg else {}
    check_interval = float(m.get("check_interval_seconds", 30))
    report_interval = float(m.get("report_interval_seconds", 3600))

    if not notifier.any_channel_enabled():
        print("[monitor] nenhum canal de notificação habilitado (.env). "
              "Thread de monitoramento não enviará mensagens.")

    last_estado = None
    last_report_ts = 0.0

    while True:
        try:
            result = compute_replica_lag()
            if "error" not in result:
                _record_replica_lag_history(result)
            estado, msg = _classify(result)
            now = time.time()

            # 1) Alerta em mudança de estado
            if last_estado is not None and estado != last_estado:
                if estado == "OK":
                    assunto = "✅ Réplica PostgreSQL recuperada"
                elif estado == "CRITICO":
                    assunto = "🚨 ALERTA: lag de replicação crítico"
                else:  # INDISPONIVEL
                    assunto = "⚠️ Réplica PostgreSQL indisponível"
                corpo = _format_report(result, estado, msg)
                res = notifier.notify(assunto, corpo)
                print(f"[monitor] mudança {last_estado} -> {estado}; notificações: {res}")

            # 2) Relatório periódico (independente do estado)
            if report_interval > 0 and (now - last_report_ts) >= report_interval:
                assunto = f"📊 Status da réplica PostgreSQL: {estado}"
                corpo = _format_report(result, estado, msg)
                res = notifier.notify(assunto, corpo)
                last_report_ts = now
                print(f"[monitor] relatório periódico enviado; notificações: {res}")

            last_estado = estado
        except Exception as e:
            print(f"[monitor] erro no loop de monitoramento: {e}")

        time.sleep(check_interval)


def _bot_status_text():
    """Texto de status atual da réplica, para responder ao comando /status."""
    result = compute_replica_lag()
    estado, msg = _classify(result)
    return _format_report(result, estado, msg)


def _handle_telegram_message(chat_id, text, name):
    """Trata um comando/mensagem recebido pelo bot (auto-inscrição)."""
    low = text.lower()

    if low in ("/start", "start", "/ajuda", "/help", "ajuda"):
        notifier.send_telegram_to(chat_id, (
            "👋 Bem-vindo ao monitor de réplica PostgreSQL.\n\n"
            "Para <b>receber os status e alertas</b>, envie a senha de acesso.\n\n"
            "Comandos:\n"
            "• <b>/status</b> — status atual (após inscrito)\n"
            "• <b>/stop</b> — cancelar inscrição"
        ))
        return

    if low in ("/stop", "/parar", "/sair"):
        removido = subscribers.remove_telegram(chat_id)
        notifier.send_telegram_to(chat_id, (
            "🚫 Inscrição cancelada. Você não receberá mais notificações."
            if removido else "Você não estava inscrito."
        ))
        return

    if low in ("/status", "status"):
        if subscribers.is_subscribed(chat_id):
            notifier.send_telegram_to(chat_id, _bot_status_text())
        else:
            notifier.send_telegram_to(chat_id, "🔒 Envie a senha de acesso primeiro para se inscrever.")
        return

    # Qualquer outra mensagem: tratada como tentativa de senha
    if notifier.TELEGRAM_SUBSCRIBE_PASSWORD and text == notifier.TELEGRAM_SUBSCRIBE_PASSWORD:
        novo = subscribers.add_telegram(chat_id, name)
        notifier.send_telegram_to(chat_id, (
            "✅ Inscrito com sucesso! Você receberá os alertas e relatórios.\nUse /status para ver agora."
            if novo else "Você já estava inscrito. Use /status para ver o estado atual."
        ))
    else:
        notifier.send_telegram_to(chat_id, "❌ Senha incorreta ou comando não reconhecido. Envie a senha de acesso ou /ajuda.")


def telegram_polling_loop():
    """Long-polling do bot para processar auto-inscrição via senha."""
    offset = None
    # Descarta updates antigos acumulados antes de iniciar (evita reprocessar)
    pendentes, err = notifier.get_updates(offset=-1, timeout=0)
    if not err and pendentes:
        offset = pendentes[-1]["update_id"] + 1

    print("[bot] polling de auto-inscrição iniciado")
    while True:
        try:
            updates, err = notifier.get_updates(offset=offset, timeout=25)
            if err:
                print(f"[bot] erro no getUpdates: {err}")
                time.sleep(5)
                continue
            for u in updates:
                offset = u["update_id"] + 1
                msg = u.get("message") or u.get("edited_message")
                if not msg:
                    continue
                chat = msg.get("chat", {})
                chat_id = chat.get("id")
                text = (msg.get("text") or "").strip()
                name = chat.get("first_name") or chat.get("title") or ""
                if chat_id is not None and text:
                    _handle_telegram_message(chat_id, text, name)
        except Exception as e:
            print(f"[bot] erro no loop de polling: {e}")
            time.sleep(5)


def start_monitor():
    """Inicia as threads de background (monitoramento + bot de inscrição)."""
    m = cfg["monitor"] if "monitor" in cfg else {}
    enabled = str(m.get("enabled", "true")).strip().lower() in ("1", "true", "yes", "on", "sim")
    if enabled:
        Thread(target=monitor_loop, name="replica-monitor", daemon=True).start()
        print("[monitor] thread de monitoramento iniciada")
    else:
        print("[monitor] desabilitado em config.ini ([monitor] enabled=false)")

    # Polling do bot só faz sentido com Telegram ligado e senha configurada
    if notifier.TELEGRAM_ENABLED and notifier.TELEGRAM_SUBSCRIBE_PASSWORD:
        Thread(target=telegram_polling_loop, name="telegram-bot", daemon=True).start()
    elif notifier.TELEGRAM_ENABLED:
        print("[bot] TELEGRAM_SUBSCRIBE_PASSWORD não definido; auto-inscrição desabilitada")


@app.route("/api/notify/test", methods=["POST", "GET"])
def api_notify_test():
    """Dispara uma notificação de teste para validar a configuração do .env."""
    res = notifier.notify(
        "🔔 Teste de notificação - pgrep_monitor",
        "Mensagem de teste disparada manualmente via /api/notify/test.",
    )
    if not res:
        return jsonify({"ok": False, "msg": "Nenhum canal habilitado no .env"}), 400
    return jsonify({"ok": True, "resultados": {k: {"ok": v[0], "detalhe": v[1]} for k, v in res.items()}})


if __name__ == "__main__":
    print("Starting Flask app on http://0.0.0.0:5050")
    # use_reloader=False evita que a thread de monitoramento seja iniciada em duplicidade
    start_monitor()
    app.run(host="0.0.0.0", port=5050, debug=True, use_reloader=False)