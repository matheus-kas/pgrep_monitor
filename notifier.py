#!/usr/bin/env python3
"""Envio de notificações de status para Telegram e e-mail.

As credenciais são lidas de variáveis de ambiente (arquivo .env, carregado
automaticamente via python-dotenv). Veja .env.example para a lista completa.
"""
import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

import subscribers

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv é opcional: se não estiver instalado, usa só o ambiente real
    pass


def _env_bool(name, default=False):
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on", "sim")


# --- Telegram ---------------------------------------------------------------
TELEGRAM_ENABLED = _env_bool("TELEGRAM_ENABLED", False)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
# chat_id "fixo" opcional (ex.: admin). Os demais destinatários vêm da
# auto-inscrição via bot (módulo subscribers).
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
# Senha compartilhada que a pessoa envia ao bot para se inscrever.
# Se vazia, a auto-inscrição fica desabilitada.
TELEGRAM_SUBSCRIBE_PASSWORD = os.getenv("TELEGRAM_SUBSCRIBE_PASSWORD", "")

# --- E-mail (SMTP) ----------------------------------------------------------
EMAIL_ENABLED = _env_bool("EMAIL_ENABLED", False)
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", SMTP_USER)
EMAIL_TO = os.getenv("EMAIL_TO", "")
SMTP_USE_TLS = _env_bool("SMTP_USE_TLS", True)


def send_telegram_to(chat_id, text):
    """Envia uma mensagem para um chat_id específico. Retorna (ok, detalhe)."""
    if not TELEGRAM_BOT_TOKEN:
        return False, "TELEGRAM_BOT_TOKEN ausente"
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            return True, "ok"
        return False, f"HTTP {resp.status_code}: {resp.text}"
    except Exception as e:
        return False, str(e)


def _telegram_recipients():
    """Conjunto de destinatários: inscritos via bot + chat_id fixo (se houver)."""
    dests = set(subscribers.list_telegram_chat_ids())
    if TELEGRAM_CHAT_ID:
        dests.add(str(TELEGRAM_CHAT_ID))
    return sorted(dests)


def send_telegram(text):
    """Envia para todos os destinatários do Telegram. Retorna (ok, detalhe).

    'ok' é True se ao menos um envio teve sucesso.
    """
    if not TELEGRAM_ENABLED:
        return False, "telegram desabilitado"
    recipients = _telegram_recipients()
    if not recipients:
        return False, "nenhum destinatário (sem inscritos e sem TELEGRAM_CHAT_ID)"
    sucessos, falhas = 0, []
    for chat_id in recipients:
        ok, detalhe = send_telegram_to(chat_id, text)
        if ok:
            sucessos += 1
        else:
            falhas.append(f"{chat_id}: {detalhe}")
    if sucessos and not falhas:
        return True, f"enviado para {sucessos} destinatário(s)"
    if sucessos:
        return True, f"enviado para {sucessos}; falhas: {'; '.join(falhas)}"
    return False, "; ".join(falhas)


def get_updates(offset=None, timeout=25):
    """Long-polling de mensagens recebidas pelo bot (getUpdates).

    Retorna (lista_de_updates, erro). Usado pelo loop de auto-inscrição.
    """
    if not TELEGRAM_BOT_TOKEN:
        return [], "TELEGRAM_BOT_TOKEN ausente"
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    params = {"timeout": timeout}
    if offset is not None:
        params["offset"] = offset
    try:
        resp = requests.get(url, params=params, timeout=timeout + 10)
        if resp.status_code != 200:
            return [], f"HTTP {resp.status_code}: {resp.text}"
        return resp.json().get("result", []), None
    except Exception as e:
        return [], str(e)


def send_email(subject, body_text, body_html=None):
    """Envia e-mail via SMTP. Retorna (ok, detalhe).

    Usa SMTP_SSL quando a porta é 465; caso contrário SMTP + STARTTLS
    (se SMTP_USE_TLS estiver ligado).
    """
    if not EMAIL_ENABLED:
        return False, "email desabilitado"
    if not SMTP_HOST or not EMAIL_TO:
        return False, "SMTP_HOST ou EMAIL_TO ausente"

    recipients = [r.strip() for r in EMAIL_TO.split(",") if r.strip()]
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(body_text, "plain", "utf-8"))
    if body_html:
        msg.attach(MIMEText(body_html, "html", "utf-8"))

    try:
        if SMTP_PORT == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context, timeout=15) as server:
                if SMTP_USER:
                    server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(EMAIL_FROM, recipients, msg.as_string())
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
                if SMTP_USE_TLS:
                    server.starttls(context=ssl.create_default_context())
                if SMTP_USER:
                    server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(EMAIL_FROM, recipients, msg.as_string())
        return True, "ok"
    except Exception as e:
        return False, str(e)


def notify(subject, text, html=None):
    """Despacha uma notificação para todos os canais habilitados.

    Retorna um dict {canal: (ok, detalhe)} apenas para os canais habilitados.
    """
    results = {}
    if TELEGRAM_ENABLED:
        tg_text = f"<b>{subject}</b>\n{text}"
        results["telegram"] = send_telegram(tg_text)
    if EMAIL_ENABLED:
        results["email"] = send_email(subject, text, html)
    return results


def any_channel_enabled():
    return TELEGRAM_ENABLED or EMAIL_ENABLED


if __name__ == "__main__":
    # Permite testar a configuração via: python notifier.py
    print("Telegram habilitado:", TELEGRAM_ENABLED)
    print("E-mail habilitado:", EMAIL_ENABLED)
    res = notify(
        "🔔 Teste de notificação",
        "Esta é uma mensagem de teste do pgrep_monitor.",
    )
    if not res:
        print("Nenhum canal habilitado. Configure o arquivo .env.")
    for canal, (ok, detalhe) in res.items():
        print(f"  {canal}: {'OK' if ok else 'FALHA'} - {detalhe}")
