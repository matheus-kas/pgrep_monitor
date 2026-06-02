#!/usr/bin/env python3
"""Ponto de entrada WSGI para produção (ex.: Gunicorn).

Uso:
    gunicorn -w 1 -b 0.0.0.0:5050 wsgi:app

Importar este módulo inicia as threads de background (monitoramento da réplica
e bot de auto-inscrição), o que NÃO acontece ao importar 'app' diretamente.
Use sempre 1 worker (-w 1): o long-polling do Telegram não admite duas
instâncias simultâneas e múltiplos workers duplicariam os alertas.
"""
from app import app, start_monitor

start_monitor()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050)
