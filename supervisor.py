# ═══════════════════════════════════════════════════════════════════════════════
# supervisor.py — Sprint 4: Reporter de Métricas para o Supervisor do Cluster
#
# Cada Master envia, a cada 10s, um `performance_report` em JSON ao Supervisor do
# professor. Canal oficial (endereço atual do professor): socket TCP PURO, sem SSL,
# em 10.62.206.206:8000 — conecta, envia o JSON + '\n' e fecha (fire-and-forget,
# NUNCA faz recv). Se a 8000 estiver fora do ar, cai para o dashboard HTTP em :5000.
#
# Tudo em try/except para jamais derrubar o Master se a rede falhar.
#
# Este módulo NÃO importa master.py (evita import circular): recebe o estado da
# farm por callback (get_state) ligado em master.collect_state.
# ═══════════════════════════════════════════════════════════════════════════════
import os
import json
import time
import uuid
import socket
import threading
import urllib.request
from datetime import datetime, timezone

import psutil

# ─────────────────────────────────────────────────────────────────────────────
# Sprint 4 — Config do Supervisor
#
# Endereço atualizado pelo professor (15/06): supervisor LOCAL via HTTP.
#   http://10.62.206.206:5000/   → POST com o JSON do performance_report.
# (A versão anterior era TLS em nuted-ia.dev:443; mantida em comentário p/ histórico.)
# ─────────────────────────────────────────────────────────────────────────────
SUP_HOST = "10.62.206.206"   # endereço do supervisor (rede da sala)
SUP_PORT = 8000              # PDF p.17 / professor — socket TCP na porta 8000, sem SSL
# Fallback: se o coletor socket:8000 estiver fora do ar, tenta o dashboard HTTP:5000.
# Deixe FALLBACK_HTTP_URL = None para enviar EXCLUSIVAMENTE pelo socket 8000.
FALLBACK_HTTP_URL = "http://10.62.206.206:5000/"
REPORT_INTERVAL = 10                   # segundos entre relatórios
PAYLOAD_VERSION = "sprint4-monitor"    # §6 — confirmar com o professor (1 linha p/ trocar)
WARN_CPU    = 85
WARN_MEMORY = 85

# Sprint 4 (extra) — espelho LOCAL p/ o dashboard.py (TCP puro). None desativa.
# Não interfere no envio oficial (TLS/443); é só uma cópia para a UI local.
LOCAL_DASHBOARD = ("127.0.0.1", 9009)


def _log(msg: str) -> None:
    """Sprint 4 — log próprio (supervisor não importa master para evitar ciclo)."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [SUPERVISOR] {msg}", flush=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 4 — Métricas de sistema (psutil), tolerantes a Windows
# ═══════════════════════════════════════════════════════════════════════════════
def _safe_loadavg():
    """getloadavg() é indisponível em alguns Windows/psutil — fallback p/ zeros."""
    try:
        return psutil.getloadavg()      # (1m, 5m, 15m)
    except (AttributeError, OSError):
        return (0.0, 0.0, 0.0)


def _safe_disk_usage():
    """disk_usage do volume raiz, tolerante a Windows ('C:\\') e Linux ('/')."""
    for path in (os.path.abspath(os.sep), "/"):
        try:
            return psutil.disk_usage(path)
        except (OSError, ValueError):
            continue
    return None


def build_system_metrics(start_time: float) -> dict:
    vm = psutil.virtual_memory()
    du = _safe_disk_usage()
    l1, l5, _ = _safe_loadavg()
    disk = {
        "total_gb":     round(du.total / 1_073_741_824, 1) if du else 0.0,
        "free_gb":      round(du.free / 1_073_741_824, 1) if du else 0.0,
        "percent_used": round(du.percent, 1) if du else 0.0,
    }
    return {
        "uptime_seconds":  int(time.time() - start_time),
        "load_average_1m": round(l1, 2),
        "load_average_5m": round(l5, 2),
        "cpu": {
            "usage_percent":  round(psutil.cpu_percent(interval=None), 2),
            "count_logical":  psutil.cpu_count(logical=True) or 0,
            "count_physical": psutil.cpu_count(logical=False) or 0,
        },
        "memory": {
            "total_mb":     int(vm.total / 1_048_576),
            "available_mb": int(vm.available / 1_048_576),
            "percent_used": round(vm.percent, 2),
            "memory_used":  int(vm.used / 1_048_576),
        },
        "disk": disk,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 4 — Montagem do payload `performance_report`
# ═══════════════════════════════════════════════════════════════════════════════
def build_payload(server_uuid, hostname, start_time, farm_state, thresholds, neighbors) -> dict:
    return {
        "server_uuid":     server_uuid,
        "hostname":        hostname,
        "role":            "master",
        "task":            "performance_report",
        "timestamp":       datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "message_id":      str(uuid.uuid4()),
        "payload_version": PAYLOAD_VERSION,
        "performance": {
            "system":            build_system_metrics(start_time),
            "farm_state":        farm_state,          # vem do callback do master
            "config_thresholds": thresholds,          # vem do callback do master
            "neighbors":         neighbors,           # vem do callback do master
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 4 — Envio por socket TCP puro (sem SSL) na porta 8000 (canal oficial)
# Fire-and-forget: conecta, envia o JSON + '\n' e fecha. NUNCA faz recv (PDF p.17).
# Se o socket:8000 estiver recusando, cai para o HTTP POST:5000 (FALLBACK_HTTP_URL).
# Retorna o canal usado ("socket:8000" ou "http:5000") para o log.
# ═══════════════════════════════════════════════════════════════════════════════
def send_report(payload: dict) -> str:
    body = (json.dumps(payload) + "\n").encode("utf-8")
    try:
        with socket.create_connection((SUP_HOST, SUP_PORT), timeout=5) as s:
            s.sendall(body)           # apenas SEND e close — sem recv
        return f"socket:{SUP_PORT}"
    except OSError as e_sock:
        if not FALLBACK_HTTP_URL:
            raise
        # Fallback: dashboard HTTP na 5000 (Flask espera um POST, não socket cru)
        req = urllib.request.Request(
            FALLBACK_HTTP_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()
        return "http:5000(fallback)"


def _mirror_local(payload: dict) -> None:
    """Sprint 4 (extra) — copia o report (TCP puro) p/ o dashboard.py local.
    Best-effort e silencioso: se o dashboard não estiver no ar, ignora."""
    if not LOCAL_DASHBOARD:
        return
    try:
        with socket.create_connection(LOCAL_DASHBOARD, timeout=1) as s:
            s.sendall((json.dumps(payload) + "\n").encode("utf-8"))
    except OSError:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 4 — Loop do reporter (thread daemon, a cada 10s; nunca derruba o Master)
# ═══════════════════════════════════════════════════════════════════════════════
def start_reporter(get_state) -> None:
    """get_state() -> (server_uuid, hostname, start_time, farm_state, thresholds, neighbors)"""
    def loop():
        while True:
            payload = None
            try:
                payload = build_payload(*get_state())
                channel = send_report(payload)
                _log(f"Reporter → {SUP_HOST} via {channel} ok "
                     f"(msg_id={payload['message_id'][:8]})")
            except Exception as e:
                # Rede pode falhar a qualquer momento: só loga, jamais propaga.
                _log(f"falha ao reportar: {e}")
            if payload:
                _mirror_local(payload)      # alimenta o dashboard local, se houver
            time.sleep(REPORT_INTERVAL)

    threading.Thread(target=loop, daemon=True).start()
