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

@app.route("/api/replica_lag")
def api_replica_lag():
    def fetch():
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
            critical = False
            if in_recovery:
                if replay_lag_seconds is None:
                    status = "EM RECUPERAÇÃO"
                    critical = True
                else:
                    if replay_lag_seconds > threshold:
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

            # Append to in-memory history (thread-safe)
            try:
                # use timezone-aware UTC timestamps to avoid deprecation warnings
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
                # don't fail the API if history maintenance fails
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

if __name__ == "__main__":
    print("Starting Flask app on http://0.0.0.0:5051")
    app.run(host="0.0.0.0", port=5050, debug=True)