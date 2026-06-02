#!/usr/bin/env python3
from flask import Flask, jsonify, render_template, send_from_directory, request
from threading import Lock, Thread
import collections
import configparser
import psycopg2
import psycopg2.extras
import time
from datetime import datetime, timezone
from html import escape as _html_escape

import notifier
import subscribers

app = Flask(__name__, template_folder="templates", static_folder="static")

# Read config.ini from same folder
cfg = configparser.ConfigParser()
cfg.read("config.ini", encoding="utf-8")

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
    # Critério principal = atraso em BYTES (WAL recebido mas ainda não aplicado).
    # O atraso em tempo é só informativo: num master ocioso ele cresce sozinho
    # mesmo com a réplica 100% sincronizada (0 bytes), gerando falso alarme.
    threshold_bytes = int(m.get("lag_threshold_bytes", 16 * 1024 * 1024))  # 16 MB
    byte_lag = result.get("exact_byte_lag")
    time_lag = result.get("replay_lag_seconds")

    if byte_lag is not None and byte_lag > threshold_bytes:
        return "CRITICO", f"WAL não aplicado: {result.get('lag_pretty')} acima do limite."
    tempo_txt = f"{round(time_lag, 1)}s" if time_lag is not None else "—"
    return "OK", f"Sincronizada (atraso {result.get('lag_pretty') or '0 B'}, tempo {tempo_txt})."


_ESTADO_INFO = {
    "OK": ("✅", "Réplica sincronizada com o master."),
    "CRITICO": ("🚨", "Atenção: a réplica está atrasada."),
    "INDISPONIVEL": ("⚠️", "Não foi possível consultar a réplica."),
}


def _titulo(estado):
    emoji, _ = _ESTADO_INFO.get(estado, ("ℹ️", ""))
    return f"{emoji} Réplica PostgreSQL — {estado}"


def _fmt_dt(value):
    """Formata um timestamp (ISO ou datetime) em horário local: dd/mm/aaaa HH:MM:SS."""
    if not value:
        return "—"
    try:
        dt = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        return dt.astimezone().strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return str(value)


def _format_report(result, estado, msg, html=False):
    """Monta a mensagem de status (sem o título — o título vai no assunto).

    html=True usa <b> e escapa valores (para o Telegram); caso contrário gera
    texto puro (para e-mail).
    """
    _, resumo = _ESTADO_INFO.get(estado, ("", msg))
    b = (lambda s: f"<b>{_html_escape(str(s))}</b>") if html else (lambda s: str(s))

    linhas = [resumo, ""]
    if "error" in result:
        linhas.append(f"Erro: {b(result['error'])}")
    else:
        modo = "standby (réplica)" if result.get("in_recovery") else "primário"
        linhas += [
            f"⏱️ Atraso (tempo): {b(str(result.get('replay_lag_seconds_rounded')) + ' s')}",
            f"💾 Atraso (dados): {b(result.get('lag_pretty') or '0 B')}",
            f"🛰️ Modo: {b(modo)}",
            f"🕒 Último replay: {b(_fmt_dt(result.get('last_replay_timestamp')))}",
        ]
    linhas.append(f"📅 Verificado: {b(_fmt_dt(datetime.now()))}")
    return "\n".join(linhas)


# --- Helpers de horário/preferências --------------------------------------

def _parse_hhmm(s):
    """'HH:MM' -> (h, m) válido, ou None."""
    try:
        h, m = str(s).strip().split(":")
        h, m = int(h), int(m)
        if 0 <= h < 24 and 0 <= m < 60:
            return h, m
    except Exception:
        pass
    return None


def _parse_horas(s):
    """'6h', '6', '1.5h' -> horas (float > 0), ou None."""
    try:
        v = float(str(s).lower().replace("h", "").replace(",", ".").strip())
        return v if v > 0 else None
    except Exception:
        return None


def _in_quiet(agora, prefs):
    """True se 'agora' (datetime local) está no horário silencioso do inscrito."""
    ini = _parse_hhmm(prefs.get("quiet_start"))
    fim = _parse_hhmm(prefs.get("quiet_end"))
    if not ini or not fim:
        return False
    minutos = agora.hour * 60 + agora.minute
    a, b = ini[0] * 60 + ini[1], fim[0] * 60 + fim[1]
    if a == b:
        return False
    if a < b:
        return a <= minutos < b
    return minutos >= a or minutos < b  # janela que cruza a meia-noite (22:00-06:00)


def _passou_horario_diario(agora, hhmm):
    """True se a hora local já alcançou hhmm hoje."""
    alvo = _parse_hhmm(hhmm)
    if not alvo:
        return False
    return agora.hour * 60 + agora.minute >= alvo[0] * 60 + alvo[1]


def monitor_loop():
    """Verifica o lag periodicamente e notifica respeitando as preferências de
    cada inscrito (alerta de mudança, re-lembrete enquanto crítico, relatório,
    horário silencioso). E-mail segue a configuração global do config.ini.
    Roda em uma thread daemon."""
    m = cfg["monitor"] if "monitor" in cfg else {}
    check_interval = float(m.get("check_interval_seconds", 30))
    email_report_interval = float(m.get("report_interval_seconds", 3600))

    if not notifier.any_channel_enabled():
        print("[monitor] nenhum canal de notificação habilitado (.env). "
              "Thread de monitoramento não enviará mensagens.")

    last_estado = None
    last_renotify = {}     # chat_id -> ts do último lembrete
    last_report_iv = {}    # chat_id -> ts do último relatório (modo intervalo)
    last_report_day = {}   # chat_id -> 'YYYY-MM-DD' do último relatório (modo diário)
    last_email_report = 0.0

    while True:
        try:
            result = compute_replica_lag()
            if "error" not in result:
                _record_replica_lag_history(result)
            estado, msg = _classify(result)
            agora = datetime.now().astimezone()
            now_ts = agora.timestamp()
            mudou = last_estado is not None and estado != last_estado

            subject = _titulo(estado)
            plain = _format_report(result, estado, msg)
            html = _format_report(result, estado, msg, html=True)

            # ----- Telegram: por inscrito, respeitando preferências -----
            if notifier.TELEGRAM_ENABLED:
                for sub in subscribers.list_subscribers():
                    cid = sub["chat_id"]
                    p = sub["prefs"]
                    silencio = _in_quiet(agora, p)

                    # 1) alerta de mudança de estado (sempre — info importante)
                    if mudou and p.get("alerts", True):
                        notifier.send_telegram_to(cid, f"<b>{subject}</b>\n{html}")

                    # 2) re-lembrete enquanto continuar crítico (respeita silêncio)
                    if estado == "CRITICO" and p.get("renotify", True) and not silencio:
                        iv = float(p.get("renotify_interval_seconds", 1800))
                        if now_ts - last_renotify.get(cid, 0) >= iv:
                            notifier.send_telegram_to(cid, f"<b>🔁 Continua crítico — {subject}</b>\n{html}")
                            last_renotify[cid] = now_ts
                    if estado != "CRITICO":
                        last_renotify.pop(cid, None)

                    # 3) relatório periódico (respeita silêncio)
                    if not silencio:
                        modo = p.get("report", "off")
                        if modo == "interval":
                            iv = float(p.get("report_interval_seconds", 3600))
                            if now_ts - last_report_iv.get(cid, 0) >= iv:
                                notifier.send_telegram_to(cid, f"<b>📊 {subject}</b>\n{html}")
                                last_report_iv[cid] = now_ts
                        elif modo == "daily":
                            hoje = agora.date().isoformat()
                            if last_report_day.get(cid) != hoje and _passou_horario_diario(agora, p.get("report_daily_at")):
                                notifier.send_telegram_to(cid, f"<b>📊 {subject}</b>\n{html}")
                                last_report_day[cid] = hoje

            # ----- E-mail: configuração global -----
            if notifier.EMAIL_ENABLED:
                if mudou:
                    notifier.send_email(subject, plain, html)
                if email_report_interval > 0 and now_ts - last_email_report >= email_report_interval:
                    notifier.send_email(f"[Relatório] {subject}", plain, html)
                    last_email_report = now_ts

            if mudou:
                print(f"[monitor] mudança {last_estado} -> {estado}")
            last_estado = estado
        except Exception as e:
            print(f"[monitor] erro no loop de monitoramento: {e}")

        time.sleep(check_interval)


def _bot_status_text():
    """Texto de status atual da réplica (HTML), para responder ao /status."""
    result = compute_replica_lag()
    estado, msg = _classify(result)
    return f"<b>{_titulo(estado)}</b>\n{_format_report(result, estado, msg, html=True)}"


_AJUDA_CONFIG = (
    "Comandos de preferência:\n"
    "• <b>/config</b> — mostra suas preferências\n"
    "• <b>/alertas</b> on|off — alerta quando muda de estado\n"
    "• <b>/lembrete</b> on [min] | off — re-lembrete enquanto crítico\n"
    "• <b>/relatorio</b> off | diario HH:MM | intervalo Nh\n"
    "• <b>/silencio</b> HH:MM HH:MM | off — não perturbar\n"
    "• <b>/status</b> — status atual  •  <b>/stop</b> — sair"
)

_MSG_BOAS_VINDAS = (
    "👋 Bem-vindo ao monitor de réplica PostgreSQL.\n\n"
    "Para <b>receber os status e alertas</b>, envie a senha de acesso.\n\n"
    "Depois de inscrito, use <b>/config</b> para personalizar o que recebe."
)


def _format_prefs(prefs):
    if prefs["report"] == "daily":
        rel = f"diário às {prefs['report_daily_at']}"
    elif prefs["report"] == "interval":
        rel = f"a cada {round(prefs['report_interval_seconds'] / 3600, 2)} h"
    else:
        rel = "desligado"
    silencio = (f"{prefs['quiet_start']}–{prefs['quiet_end']}"
                if prefs.get("quiet_start") and prefs.get("quiet_end") else "desligado")
    lembrete = (f"a cada {round(prefs['renotify_interval_seconds'] / 60)} min"
                if prefs["renotify"] else "desligado")
    return (
        "⚙️ <b>Suas preferências</b>\n"
        f"• Alertas de mudança: {'on' if prefs['alerts'] else 'off'}\n"
        f"• Lembrete enquanto crítico: {lembrete}\n"
        f"• Relatório: {rel}\n"
        f"• Horário silencioso: {silencio}"
    )


def _handle_config_command(chat_id, cmd, parts):
    """Processa comandos de preferência (exigem inscrição prévia)."""
    args = parts[1:]

    if cmd in ("/config", "/configuracao", "/preferencias"):
        notifier.send_telegram_to(chat_id, _format_prefs(subscribers.get_prefs(chat_id)) + "\n\n" + _AJUDA_CONFIG)

    elif cmd == "/alertas":
        if args and args[0] in ("on", "off"):
            subscribers.set_pref(chat_id, "alerts", args[0] == "on")
            notifier.send_telegram_to(chat_id, f"✅ Alertas de mudança: {args[0]}")
        else:
            notifier.send_telegram_to(chat_id, "Uso: <b>/alertas on|off</b>")

    elif cmd == "/lembrete":
        if args and args[0] == "off":
            subscribers.set_pref(chat_id, "renotify", False)
            notifier.send_telegram_to(chat_id, "✅ Lembrete enquanto crítico: off")
        elif args and args[0] == "on":
            subscribers.set_pref(chat_id, "renotify", True)
            if len(args) > 1 and args[1].isdigit() and int(args[1]) > 0:
                subscribers.set_pref(chat_id, "renotify_interval_seconds", int(args[1]) * 60)
                notifier.send_telegram_to(chat_id, f"✅ Lembrete: on, a cada {int(args[1])} min")
            else:
                notifier.send_telegram_to(chat_id, "✅ Lembrete enquanto crítico: on")
        else:
            notifier.send_telegram_to(chat_id, "Uso: <b>/lembrete on [minutos] | off</b>")

    elif cmd in ("/relatorio", "/relatório"):
        if args and args[0] == "off":
            subscribers.set_pref(chat_id, "report", "off")
            notifier.send_telegram_to(chat_id, "✅ Relatório: desligado")
        elif args and args[0] in ("diario", "diário") and len(args) > 1 and _parse_hhmm(args[1]):
            subscribers.set_pref(chat_id, "report", "daily")
            subscribers.set_pref(chat_id, "report_daily_at", args[1])
            notifier.send_telegram_to(chat_id, f"✅ Relatório diário às {args[1]}")
        elif args and args[0] == "intervalo" and len(args) > 1 and _parse_horas(args[1]):
            horas = _parse_horas(args[1])
            subscribers.set_pref(chat_id, "report", "interval")
            subscribers.set_pref(chat_id, "report_interval_seconds", int(horas * 3600))
            notifier.send_telegram_to(chat_id, f"✅ Relatório a cada {horas} h")
        else:
            notifier.send_telegram_to(chat_id, "Uso: <b>/relatorio off | diario HH:MM | intervalo Nh</b>")

    elif cmd in ("/silencio", "/silêncio"):
        if args and args[0] == "off":
            subscribers.set_pref(chat_id, "quiet_start", "")
            subscribers.set_pref(chat_id, "quiet_end", "")
            notifier.send_telegram_to(chat_id, "✅ Horário silencioso: desligado")
        elif len(args) >= 2 and _parse_hhmm(args[0]) and _parse_hhmm(args[1]):
            subscribers.set_pref(chat_id, "quiet_start", args[0])
            subscribers.set_pref(chat_id, "quiet_end", args[1])
            notifier.send_telegram_to(chat_id, f"✅ Horário silencioso: {args[0]}–{args[1]}")
        else:
            notifier.send_telegram_to(chat_id, "Uso: <b>/silencio HH:MM HH:MM | off</b>")


_CONFIG_CMDS = ("/config", "/configuracao", "/preferencias", "/alertas",
                "/lembrete", "/relatorio", "/relatório", "/silencio", "/silêncio")


def _handle_telegram_message(chat_id, text, name):
    """Trata um comando/mensagem recebido pelo bot."""
    parts = text.split()
    cmd = parts[0].lower() if parts else ""

    if cmd in ("/start", "start", "/ajuda", "/help", "ajuda"):
        notifier.send_telegram_to(chat_id, _MSG_BOAS_VINDAS)
        return

    if cmd in ("/stop", "/parar", "/sair"):
        removido = subscribers.remove_telegram(chat_id)
        notifier.send_telegram_to(chat_id, (
            "🚫 Inscrição cancelada. Você não receberá mais notificações."
            if removido else "Você não estava inscrito."
        ))
        return

    if cmd in ("/status", "status"):
        if subscribers.is_subscribed(chat_id):
            notifier.send_telegram_to(chat_id, _bot_status_text())
        else:
            notifier.send_telegram_to(chat_id, "🔒 Envie a senha de acesso primeiro para se inscrever.")
        return

    if cmd in _CONFIG_CMDS:
        if subscribers.is_subscribed(chat_id):
            _handle_config_command(chat_id, cmd, parts)
        else:
            notifier.send_telegram_to(chat_id, "🔒 Envie a senha de acesso primeiro para se inscrever.")
        return

    # Qualquer outra mensagem: tratada como tentativa de senha
    if notifier.TELEGRAM_SUBSCRIBE_PASSWORD and text == notifier.TELEGRAM_SUBSCRIBE_PASSWORD:
        novo = subscribers.add_telegram(chat_id, name)
        notifier.send_telegram_to(chat_id, (
            "✅ Inscrito com sucesso! Você receberá os alertas.\n"
            "Use <b>/config</b> para personalizar e <b>/status</b> para ver agora."
            if novo else "Você já estava inscrito. Use /status ou /config."
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


_MONITOR_STARTED = False
_MONITOR_START_LOCK = Lock()


def start_monitor():
    """Inicia as threads de background (monitoramento + bot de inscrição).

    Idempotente: chamadas repetidas (ex.: dev server + wsgi) não duplicam as
    threads. Em produção, garanta um único processo/worker (ver README), pois
    o long-polling do Telegram não admite duas instâncias simultâneas.
    """
    global _MONITOR_STARTED
    with _MONITOR_START_LOCK:
        if _MONITOR_STARTED:
            return
        _MONITOR_STARTED = True

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