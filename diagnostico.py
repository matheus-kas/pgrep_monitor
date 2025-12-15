#!/usr/bin/env python3
import sys
import subprocess
import os

def diagnosticar_python():
    print("=== DIAGNÓSTICO DO AMBIENTE PYTHON ===\n")
    
    # Versão do Python
    print(f"Python version: {sys.version}")
    print(f"Executável: {sys.executable}")
    print(f"Prefix: {sys.prefix}")
    
    # Verificar bibliotecas
    bibliotecas = ['psycopg2', 'pandas', 'openpyxl', 'tkinter']
    
    print(f"\n=== BIBLIOTECAS ===")
    for lib in bibliotecas:
        try:
            if lib == 'tkinter':
                import tkinter
                version = "OK"
            else:
                module = __import__(lib)
                version = getattr(module, '__version__', 'OK')
            print(f"✅ {lib}: {version}")
        except ImportError as e:
            print(f"❌ {lib}: FALHA - {e}")
    
    # Caminhos do Python
    print(f"\n=== CAMINHOS DO SISTEMA ===")
    python_paths = [
        "/usr/bin/python3",
        "/Library/Frameworks/Python.framework/Versions/3.12/bin/python3",
        "/usr/local/bin/python3"
    ]
    
    for path in python_paths:
        if os.path.exists(path):
            try:
                result = subprocess.run([path, '--version'], capture_output=True, text=True)
                print(f"✅ {path}: {result.stdout.strip()}")
            except:
                print(f"❌ {path}: Inacessível")
        else:
            print(f"❌ {path}: Não encontrado")

if __name__ == "__main__":
    diagnosticar_python()