#!/usr/bin/env python3
from flask import Flask, jsonify, render_template, send_from_directory, request
from threading import Lock
import collections
import configparser
import psycopg2
import psycopg2.extras
import time
from datetime import datetime, timezone

app = Flask(__name__, template_folder="templates", static_folder="static")

_cache = {}
CACHE_TTL = 2.0

HISTORY_REPLICA_LAG = collections.deque()
HISTORY_LOCK = Lock()
HISTORY_MAX_SAMPLES = 720
HISTORY_RETENTION_SECONDS = 3600


def get_conn(section):
    local_cfg = configparser.ConfigParser()
    local_cfg.read("config.ini")
    if section not in local_cfg:
        raise RuntimeError(f"Seção {section} não encontrada no config.ini")
    s = local_cfg[section]
    conn = psycopg2.connect(
        host=s.get("host"),
        port=s.get("port", 5432),
        user=s.get("user"),
        password=s.get("password"),
        dbname=s.get("database", "postgres"),
        connect_timeout=5,
        options="-c client_encoding=UTF8"
    )
    return conn


def get_thresholds():
    local_cfg = configparser.ConfigParser()
    local_cfg.read("config.ini")
    m = local_cfg["monitor"] if "monitor" in local_cfg else {}
    return {
        "lag_warn_seconds":     float(m.get("lag_threshold_seconds",  30)),
        "lag_critical_seconds": float(m.get("lag_critical_seconds",   300)),
        "lag_warn_bytes":       float(m.get("lag_threshold_bytes",     16777216)),
        "lag_critical_bytes":   float(m.get("lag_critical_bytes",      524288000)),
        "cleanup_warn_min":     float(m.get("cleanup_warn_minutes",    90)),
        "cleanup_critical_min": float(m.get("cleanup_critical_minutes", 180)),
    }


def cached(key, fn, ttl=None):
    now = time.time()
    effective_ttl = ttl if ttl is not None else CACHE_TTL
    entry = _cache.get(key)
    if entry and (now - entry["t"] < effective_ttl):
        return entry["v"]
    v = fn()
    _cache[key] = {"v": v, "t": now}
    return v


def pretty_bytes(b):
    if b is None:
        return None
    b = float(b)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024.0
    return f"{b:.1f} PB"


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
                out[section]["server_ip"] = cur.fetchone()[0]
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
            cur.execute("SELECT pg_is_in_recovery();")
            in_recovery = cur.fetchone()[0]

            cur.execute("""
                SELECT
                    pg_last_wal_receive_lsn(),
                    pg_last_wal_replay_lsn(),
                    pg_last_xact_replay_timestamp()
            """)
            receive_lsn, replay_lsn, last_xact_replay_ts = cur.fetchone()

            current_lsn = None
            if not in_recovery:
                cur.execute("SELECT pg_current_wal_lsn();")
                current_lsn = cur.fetchone()[0]

            cur.close()
            conn.close()

            master_current_lsn = None
            master_server_time = None
            try:
                mconn = get_conn("master")
                mcur = mconn.cursor()
                mcur.execute("SELECT pg_current_wal_lsn(), current_timestamp")
                row = mcur.fetchone()
                master_current_lsn = row[0]
                master_server_time = row[1].isoformat() if row[1] else None
                mcur.close()
                mconn.close()
            except Exception as e:
                print(f"[debug] não foi possível obter current_lsn do master: {e}")

            return {
                "is_standby": bool(in_recovery),
                "current_lsn": current_lsn,
                "receive_lsn": receive_lsn,
                "replay_lsn": replay_lsn,
                "last_replay_time": last_xact_replay_ts.isoformat() if last_xact_replay_ts else None,
                "master_current_lsn": master_current_lsn,
                "master_server_time": master_server_time,
            }
        except Exception as e:
            return {"error": str(e)}
    return jsonify(cached("replica_mode", fetch))


@app.route("/api/replica_lag")
def api_replica_lag():
    def fetch():
        try:
            conn = get_conn("replica")
            cur = conn.cursor()
            cur.execute("SELECT pg_is_in_recovery()")
            in_recovery = cur.fetchone()[0]

            cur.execute("""
                SELECT
                    pg_last_xact_replay_timestamp(),
                    EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())),
                    pg_last_wal_receive_lsn(),
                    pg_last_wal_replay_lsn(),
                    CASE
                      WHEN pg_last_wal_receive_lsn() IS NULL OR pg_last_wal_replay_lsn() IS NULL THEN NULL
                      ELSE pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())::bigint
                    END
            """)
            last_replay_ts, replay_lag_seconds, receive_lsn, replay_lsn, exact_byte_lag = cur.fetchone()
            cur.close()
            conn.close()

            lag_pretty = pretty_bytes(exact_byte_lag) if exact_byte_lag is not None else None
            t = get_thresholds()
            byte_lag_val = exact_byte_lag or 0

            if not in_recovery:
                alert_level = "ok"
                status = "PRIMARY"
                status_pt = "Servidor principal ativo"
            elif receive_lsn is None:
                alert_level = "critical"
                status = "DESCONECTADO"
                status_pt = "Réplica não está recebendo WAL do master"
            elif replay_lag_seconds is None:
                alert_level = "warn"
                status = "AGUARDANDO"
                status_pt = "Aguardando transações para calcular lag"
            elif replay_lag_seconds > t["lag_critical_seconds"] or byte_lag_val > t["lag_critical_bytes"]:
                alert_level = "critical"
                status = "CRITICO"
                status_pt = f"Atraso crítico: {replay_lag_seconds:.0f}s / {lag_pretty or 'N/A'}"
            elif replay_lag_seconds > t["lag_warn_seconds"] or byte_lag_val > t["lag_warn_bytes"]:
                alert_level = "warn"
                status = "ATENCAO"
                status_pt = f"Atraso: {replay_lag_seconds:.0f}s / {lag_pretty or 'N/A'}"
            else:
                alert_level = "ok"
                status = "SINCRONIZADO"
                status_pt = "Replicação em dia"

            result = {
                "in_recovery": in_recovery,
                "last_replay_timestamp": last_replay_ts.isoformat() if last_replay_ts else None,
                "replay_lag_seconds": float(replay_lag_seconds) if replay_lag_seconds is not None else None,
                "replay_lag_seconds_rounded": round(replay_lag_seconds, 1) if replay_lag_seconds is not None else None,
                "receive_lsn": receive_lsn,
                "replay_lsn": replay_lsn,
                "exact_byte_lag": int(exact_byte_lag) if exact_byte_lag is not None else None,
                "lag_pretty": lag_pretty,
                "status": status,
                "status_pt": status_pt,
                "alert_level": alert_level,
                "thresholds": {
                    "warn_seconds": t["lag_warn_seconds"],
                    "critical_seconds": t["lag_critical_seconds"],
                    "warn_bytes": t["lag_warn_bytes"],
                    "critical_bytes": t["lag_critical_bytes"],
                },
            }

            try:
                now_utc = datetime.now(timezone.utc)
                sample = {
                    "ts": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "replay_lag_seconds": result.get("replay_lag_seconds"),
                    "exact_byte_lag": result.get("exact_byte_lag"),
                }
                with HISTORY_LOCK:
                    HISTORY_REPLICA_LAG.append(sample)
                    while len(HISTORY_REPLICA_LAG) > HISTORY_MAX_SAMPLES:
                        HISTORY_REPLICA_LAG.popleft()
                    cutoff = datetime.now(timezone.utc).timestamp() - HISTORY_RETENTION_SECONDS
                    while HISTORY_REPLICA_LAG and (
                        datetime.fromisoformat(HISTORY_REPLICA_LAG[0]["ts"].replace("Z", "")).timestamp() < cutoff
                    ):
                        HISTORY_REPLICA_LAG.popleft()
            except Exception:
                pass

            return result
        except Exception as e:
            return {"error": str(e)}
    return jsonify(cached("replica_lag", fetch))


@app.route("/api/replication_status")
def api_replication_status():
    def fetch():
        try:
            conn = get_conn("master")
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("""
                SELECT application_name, client_addr, state, sync_state,
                  write_lag, flush_lag, replay_lag, backend_start,
                  now() - backend_start AS connection_duration
                FROM pg_stat_replication
            """)
            rows = cur.fetchall()
            cur.close()
            conn.close()

            clients = []
            for r in rows:
                conn_dur = r["connection_duration"].total_seconds() if r["connection_duration"] else None
                clients.append({
                    "application_name": r["application_name"],
                    "client_addr": str(r["client_addr"]),
                    "state": r["state"],
                    "sync_state": r["sync_state"],
                    "write_lag": str(r["write_lag"]) if r["write_lag"] else None,
                    "flush_lag": str(r["flush_lag"]) if r["flush_lag"] else None,
                    "replay_lag": str(r["replay_lag"]) if r["replay_lag"] else None,
                    "backend_start": r["backend_start"].isoformat() if r["backend_start"] else None,
                    "connection_duration_seconds": conn_dur,
                })

            connected = len(clients) > 0
            # state pode ser null quando o usuário replicator não tem pg_monitor —
            # nesse caso consideramos streaming se o cliente está presente e receive_lsn
            # não é null (verificado no endpoint replica_lag).
            # Regra: só crítico se não há nenhum cliente conectado.
            streaming = any(c["state"] == "streaming" for c in clients)
            state_visible = any(c["state"] is not None for c in clients)
            if connected and not state_visible:
                # colunas null por falta de permissão — presença do cliente é suficiente
                alert_level = "ok"
                streaming = True
            else:
                alert_level = "ok" if (connected and streaming) else ("warn" if connected else "critical")

            return {
                "clients": clients,
                "connected": connected,
                "streaming": streaming,
                "state_visible": state_visible,
                "alert_level": alert_level,
            }
        except Exception as e:
            return {"error": str(e)}
    return jsonify(cached("replication_status", fetch))


@app.route("/api/replica_lag/history")
def api_replica_lag_history():
    try:
        limit = int(request.args.get("limit", 200))
        with HISTORY_LOCK:
            items = list(HISTORY_REPLICA_LAG)[-limit:]
        return jsonify(items)
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/archive_cleanup")
def api_archive_cleanup():
    def fetch():
        try:
            conn = get_conn("master")
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("""
                SELECT id, executed_at, wal_file, status, message
                FROM archive_cleanup_log
                ORDER BY executed_at DESC
                LIMIT 20
            """)
            rows = cur.fetchall()
            entries = []
            for r in rows:
                entries.append({
                    "id": r["id"],
                    "executed_at": r["executed_at"].isoformat() if r["executed_at"] else None,
                    "wal_file": r["wal_file"],
                    "status": r["status"],
                    "message": r["message"],
                })
            cur.close()
            conn.close()

            last_success = next((e for e in entries if e["status"] == "success"), None)
            last_error   = next((e for e in entries if e["status"] == "error"),   None)
            last_run_error = bool(entries and entries[0]["status"] == "error")

            t = get_thresholds()
            now_utc = datetime.now(timezone.utc)

            minutes_since = None
            if last_success and last_success["executed_at"]:
                last_ts = datetime.fromisoformat(last_success["executed_at"])
                if last_ts.tzinfo is None:
                    last_ts = last_ts.replace(tzinfo=timezone.utc)
                minutes_since = (now_utc - last_ts).total_seconds() / 60

            if not entries:
                alert_level = "warn"
                cleanup_status_pt = "Sem registros — execute o script no master"
            elif minutes_since is None:
                alert_level = "critical"
                cleanup_status_pt = "Nenhuma limpeza bem-sucedida encontrada"
            elif minutes_since > t["cleanup_critical_min"]:
                h, m_ = int(minutes_since // 60), int(minutes_since % 60)
                alert_level = "critical"
                cleanup_status_pt = f"Sem limpeza há {h}h {m_}min — verificar cron no master"
            elif minutes_since > t["cleanup_warn_min"]:
                h, m_ = int(minutes_since // 60), int(minutes_since % 60)
                alert_level = "warn"
                cleanup_status_pt = f"Limpeza atrasada — última há {h}h {m_}min"
            elif last_run_error:
                alert_level = "warn"
                cleanup_status_pt = f"Último run com erro — limpeza OK há {int(minutes_since)}min"
            else:
                alert_level = "ok"
                cleanup_status_pt = f"OK — última limpeza há {int(minutes_since)}min"

            return {
                "entries": entries,
                "last_success": last_success,
                "last_error": last_error,
                "minutes_since_last_success": round(minutes_since, 1) if minutes_since is not None else None,
                "alert_level": alert_level,
                "cleanup_status_pt": cleanup_status_pt,
            }
        except Exception as e:
            return {"error": str(e)}
    return jsonify(cached("archive_cleanup", fetch, ttl=60.0))


@app.route("/static/<path:path>")
def static_files(path):
    return send_from_directory("static", path)


if __name__ == "__main__":
    print("Starting Flask app on http://0.0.0.0:5050")
    app.run(host="0.0.0.0", port=5050, debug=True)
