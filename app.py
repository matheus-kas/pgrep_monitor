#!/usr/bin/env python3
from flask import Flask, jsonify, render_template, send_from_directory
import configparser
import psycopg2
import psycopg2.extras
import time
from datetime import datetime

app = Flask(__name__, template_folder="templates", static_folder="static")

# Read config.ini from same folder
cfg = configparser.ConfigParser()
cfg.read("config.ini")

# Simple in-memory cache to avoid hammering DB every 5s
_cache = {}
CACHE_TTL = 2.0  # seconds

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

            return {
                "is_standby": bool(in_recovery),
                "current_lsn": current_lsn,
                "receive_lsn": receive_lsn,
                "replay_lsn": replay_lsn,
                "last_replay_time": last_xact_replay_ts.isoformat() if last_xact_replay_ts else None
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

            return {
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
            clients = []
            for r in rows:
                conn_dur = r['connection_duration'].total_seconds() if r['connection_duration'] is not None else None
                clients.append({
                    "application_name": r['application_name'],
                    "client_addr": str(r['client_addr']),
                    "state": r['state'],
                    "sync_state": r['sync_state'],
                    "write_lag": str(r['write_lag']),
                    "flush_lag": str(r['flush_lag']),
                    "replay_lag": str(r['replay_lag']),
                    "backend_start": r['backend_start'].isoformat() if r['backend_start'] else None,
                    "connection_duration_seconds": conn_dur
                })
            return clients
        except Exception as e:
            return {"error": str(e)}
    return jsonify(cached("replication_status", fetch))

@app.route("/static/<path:path>")
def static_files(path):
    return send_from_directory("static", path)

if __name__ == "__main__":
    print("Starting Flask app on http://0.0.0.0:5050")
    app.run(host="0.0.0.0", port=5050, debug=True)
