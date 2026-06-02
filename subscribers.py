#!/usr/bin/env python3
"""Gerência da lista de inscritos (e suas preferências) para notificações.

Os inscritos são pessoas que se autenticaram via bot do Telegram (enviando a
senha compartilhada). Cada inscrito tem preferências próprias (tipos de
notificação, relatório, horário silencioso). Tudo é persistido em um arquivo
JSON local (subscribers.json), com acesso protegido por lock para uso em threads.
"""
import json
import os
import threading
from datetime import datetime, timezone

SUBSCRIBERS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "subscribers.json")

_LOCK = threading.Lock()

# Preferências padrão de cada inscrito. set_pref só aceita estas chaves.
DEFAULT_PREFS = {
    "alerts": True,                      # alerta quando muda de estado
    "renotify": True,                    # re-lembrete enquanto continuar crítico
    "renotify_interval_seconds": 1800,   # 30 min entre lembretes
    "report": "off",                     # "off" | "daily" | "interval"
    "report_daily_at": "08:00",          # report = "daily": "HH:MM" ou lista ["09:00","18:00"]
    "report_interval_seconds": 3600,     # usado quando report = "interval"
    "quiet_start": "",                   # "HH:MM" ou "" (desligado)
    "quiet_end": "",                     # "HH:MM" ou ""
}


def _load():
    if not os.path.exists(SUBSCRIBERS_FILE):
        return {"telegram": []}
    try:
        with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        data.setdefault("telegram", [])
        return data
    except Exception:
        return {"telegram": []}


def _save(data):
    tmp = SUBSCRIBERS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, SUBSCRIBERS_FILE)


def _merge_prefs(stored):
    """Mescla as prefs salvas sobre os padrões (campos faltantes herdam o default)."""
    p = dict(DEFAULT_PREFS)
    p.update(stored or {})
    return p


def list_telegram_chat_ids():
    """Retorna a lista de chat_ids (strings) inscritos no Telegram."""
    with _LOCK:
        data = _load()
        return [str(s["chat_id"]) for s in data.get("telegram", [])]


def list_subscribers():
    """Retorna [{chat_id, name, since, prefs}] com prefs já mescladas."""
    with _LOCK:
        data = _load()
        out = []
        for s in data.get("telegram", []):
            out.append({
                "chat_id": str(s["chat_id"]),
                "name": s.get("name", ""),
                "since": s.get("since"),
                "prefs": _merge_prefs(s.get("prefs")),
            })
        return out


def is_subscribed(chat_id):
    chat_id = str(chat_id)
    with _LOCK:
        data = _load()
        return any(str(s["chat_id"]) == chat_id for s in data.get("telegram", []))


def get_prefs(chat_id):
    """Preferências (mescladas com o padrão) de um inscrito, ou None se não inscrito."""
    chat_id = str(chat_id)
    with _LOCK:
        data = _load()
        for s in data.get("telegram", []):
            if str(s["chat_id"]) == chat_id:
                return _merge_prefs(s.get("prefs"))
    return None


def set_pref(chat_id, key, value):
    """Atualiza uma preferência do inscrito. Retorna True se aplicou.

    Rejeita chaves desconhecidas (não pertencentes a DEFAULT_PREFS) ou inscrito
    inexistente.
    """
    if key not in DEFAULT_PREFS:
        return False
    chat_id = str(chat_id)
    with _LOCK:
        data = _load()
        for s in data.get("telegram", []):
            if str(s["chat_id"]) == chat_id:
                s.setdefault("prefs", {})[key] = value
                _save(data)
                return True
    return False


def add_telegram(chat_id, name=None):
    """Inscreve um chat_id (com prefs padrão). True se inscreveu, False se já existia."""
    chat_id = str(chat_id)
    with _LOCK:
        data = _load()
        for s in data.get("telegram", []):
            if str(s["chat_id"]) == chat_id:
                return False
        data["telegram"].append({
            "chat_id": chat_id,
            "name": name or "",
            "since": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "prefs": dict(DEFAULT_PREFS),
        })
        _save(data)
        return True


def remove_telegram(chat_id):
    """Remove um chat_id. Retorna True se removeu, False se não estava inscrito."""
    chat_id = str(chat_id)
    with _LOCK:
        data = _load()
        antes = len(data.get("telegram", []))
        data["telegram"] = [s for s in data.get("telegram", []) if str(s["chat_id"]) != chat_id]
        if len(data["telegram"]) == antes:
            return False
        _save(data)
        return True
