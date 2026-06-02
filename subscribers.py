#!/usr/bin/env python3
"""Gerência da lista de inscritos para receber notificações.

Os inscritos são pessoas que se autenticaram via bot do Telegram (enviando a
senha compartilhada). A lista é persistida em um arquivo JSON local
(subscribers.json), com acesso protegido por lock para uso em threads.
"""
import json
import os
import threading
from datetime import datetime, timezone

SUBSCRIBERS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "subscribers.json")

_LOCK = threading.Lock()


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


def list_telegram_chat_ids():
    """Retorna a lista de chat_ids (strings) inscritos no Telegram."""
    with _LOCK:
        data = _load()
        return [str(s["chat_id"]) for s in data.get("telegram", [])]


def is_subscribed(chat_id):
    chat_id = str(chat_id)
    with _LOCK:
        data = _load()
        return any(str(s["chat_id"]) == chat_id for s in data.get("telegram", []))


def add_telegram(chat_id, name=None):
    """Inscreve um chat_id. Retorna True se inscreveu, False se já existia."""
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
