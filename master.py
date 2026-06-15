import sys
import socket
import threading
import json
import time
import uuid
import queue
from collections import deque
from datetime import datetime, timezone

# Garante UTF-8 na saída: evita UnicodeEncodeError com →, ⚠️, ✅ e acentos quando
# o stdout não é UTF-8 (console cp1252 ou saída redirecionada para arquivo/pipe).
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

import supervisor   # Sprint 4 — reporter de métricas (TLS) ao Supervisor do cluster

# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 1 — Tarefa 01: Infraestrutura TCP (Workers)
# ═══════════════════════════════════════════════════════════════════════════════
MASTER_ID   = "Master_A"
WORKER_HOST = "192.168.15.178"
WORKER_PORT = 10000          # Workers conectam aqui

# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 3 — Tarefa 01: Servidor TCP entre Masters (M2M)
# ═══════════════════════════════════════════════════════════════════════════════
MASTER_HOST = "192.168.15.178"
MASTER_PORT = 10001          # Masters vizinhos conectam aqui

# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 3 — Tarefa 02: Thresholds com Histerese
# ═══════════════════════════════════════════════════════════════════════════════
CAPACITY             = 10   # capacidade nominal da fila
SATURATION_THRESHOLD = 10   # load > este → solicitar ajuda
RELEASE_THRESHOLD    = 4    # load < este → devolver workers

# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 3 — Tarefa 01: Diretório de Masters Vizinhos
# ═══════════════════════════════════════════════════════════════════════════════
NEIGHBOR_MASTERS: dict = {
    # "Master_B": {"host": "10.x.x.x", "master_port": 10011, "worker_port": 10010}
}

# ─────────────────────────────────────────────────────────────────────────────
# Estado compartilhado
# ─────────────────────────────────────────────────────────────────────────────
task_queue  = queue.Queue()    # Sprint 2 T02 — fila de tarefas (thread-safe)
state_lock  = threading.Lock() # Sprint 3 T06 — protege dicionários compartilhados

# workers: {worker_uuid: {"conn", "addr", "is_borrowed", "original_master", "send_lock"}}
workers: dict = {}

# pending_m2m: {request_id: {"event": Event, "response": dict|None}}
pending_m2m: dict = {}         # Sprint 3 T03 — correlação de respostas M2M

# conexões M2M abertas: {master_id: (conn, send_lock)}
m2m_conns: dict = {}           # Sprint 3 T05 — reutilizadas para notify_worker_returned

# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 4 — Reporter de Métricas: estado e contadores instrumentados
# ═══════════════════════════════════════════════════════════════════════════════
START_TIME  = time.time()                  # → uptime_seconds (supervisor.build_system_metrics)
# PDF p.23: michel_1/michel_2 DEVEM ser usados no server_uuid do payload do supervisor
# (identidade no dashboard do professor; separado do MASTER_ID usado no protocolo interno).
# Use "michel_2" neste master se o outro nó da dupla for "michel_1".
SERVER_UUID = "michel_1"
HOSTNAME    = f"{SERVER_UUID}.farm.local"

metrics_lock = threading.Lock()             # protege o dict `metrics`
metrics = {                                 # contadores de desempenho da farm
    "tasks_completed": 0,
    "tasks_failed":    0,
    "tasks_running":   0,
    "workers_failed":  0,
}

lent_out: dict = {}                         # {worker_uuid: peer_master_id} — emprestados p/ fora
task_timestamps = deque()                   # time.time() de cada tarefa enfileirada (idade da + antiga)

# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 3 — Tarefa 07: Logs e Observabilidade
# ═══════════════════════════════════════════════════════════════════════════════
def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [MASTER {MASTER_ID}] {msg}", flush=True)


def safe_send(conn: socket.socket, send_lock: threading.Lock, data: dict) -> bool:
    """Sprint 3 T06 — envia JSON + '\\n' de forma thread-safe (send_lock)."""
    with send_lock:
        try:
            conn.send((json.dumps(data) + "\n").encode("utf-8"))
            return True
        except Exception as e:
            log(f"Erro ao enviar: {e}")
            return False


def iter_messages(conn: socket.socket):
    """Sprint 1 T01 — lê linhas JSON delimitadas por '\\n' de uma conexão persistente."""
    buffer = ""
    while True:
        try:
            data = conn.recv(4096).decode("utf-8")
            if not data:
                break
            buffer += data
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as e:
                    log(f"JSON inválido (ignorado): {e} | conteúdo: {line[:80]}")
        except Exception as e:
            log(f"Conexão encerrada: {e}")
            break


def worker_count() -> str:
    """Sprint 3 T07 — contador de workers locais vs emprestados."""
    with state_lock:
        local    = sum(1 for w in workers.values() if not w["is_borrowed"])
        borrowed = sum(1 for w in workers.values() if w["is_borrowed"])
    return f"locais={local} emprestados={borrowed} total={local+borrowed}"


# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 1 — Tarefa 03: Responder HEARTBEAT com ALIVE
# Sprint 2 — Tarefa 01: Apresentação e Identificação (ALIVE + WORKER_UUID)
# Sprint 2 — Tarefa 02: Distribuição de Carga (QUERY / NO_TASK)
# Sprint 2 — Tarefa 03: Receber STATUS do Worker
# Sprint 2 — Tarefa 04: Enviar ACK e Persistência
# Sprint 3 — Tarefa 04: Registrar Worker temporário (register_temporary_worker)
# ═══════════════════════════════════════════════════════════════════════════════
def handle_worker(conn: socket.socket, addr: tuple) -> None:
    log(f"[W] Nova conexão: {addr}")
    send_lock   = threading.Lock()   # Sprint 3 T06 — lock por conexão
    worker_uuid = None
    is_borrowed = False

    for msg in iter_messages(conn):
        task   = msg.get("TASK", "")
        worker = msg.get("WORKER", "")
        status = msg.get("STATUS", "")
        mtype  = msg.get("type", "")

        # ── Sprint 1 T03: Responder HEARTBEAT com ALIVE ───────────────────────
        if task == "HEARTBEAT":
            safe_send(conn, send_lock, {
                "SERVER_UUID": MASTER_ID,
                "TASK":        "HEARTBEAT",
                "RESPONSE":    "ALIVE",
            })
            log(f"[W] HEARTBEAT respondido → {addr}")

        # ── Sprint 3 T04: Worker temporário se registra ───────────────────────
        elif mtype == "register_temporary_worker":
            p = msg.get("payload", {})
            if not p.get("worker_id") or not p.get("original_master_address"):
                log(f"[W] register_temporary_worker com campos obrigatórios ausentes: {msg}")
                continue
            worker_uuid = p["worker_id"]
            original    = p["original_master_address"]
            is_borrowed = True
            with state_lock:
                workers[worker_uuid] = {
                    "conn":            conn,
                    "addr":            addr,
                    "is_borrowed":     True,
                    "original_master": original,
                    "send_lock":       send_lock,
                }
            log(f"[W] Worker emprestado registrado: {worker_uuid} | origem: {original}")
            log(f"[W] Farm → {worker_count()}")

        # ── Sprint 2 T01: Apresentação ALIVE + identificação ─────────────────
        elif worker == "ALIVE":
            worker_uuid = msg.get("WORKER_UUID")
            server_uuid = msg.get("SERVER_UUID")   # presente somente se emprestado

            if not worker_uuid:
                log(f"[W] ALIVE sem WORKER_UUID ignorado: {msg}")
                continue

            if server_uuid:
                is_borrowed = True
                with state_lock:
                    workers[worker_uuid] = {
                        "conn":            conn,
                        "addr":            addr,
                        "is_borrowed":     True,
                        "original_master": server_uuid,
                        "send_lock":       send_lock,
                    }
                log(f"[W] Worker emprestado {worker_uuid} (de {server_uuid}) solicita tarefa")
            else:
                is_borrowed = False
                with state_lock:
                    workers[worker_uuid] = {
                        "conn":            conn,
                        "addr":            addr,
                        "is_borrowed":     False,
                        "original_master": None,
                        "send_lock":       send_lock,
                    }
                log(f"[W] Worker local {worker_uuid} solicita tarefa")

            log(f"[W] Farm → {worker_count()}")

            # ── Sprint 2 T02: Entregar tarefa ou informar fila vazia ──────────
            try:
                task_data = task_queue.get_nowait()
                safe_send(conn, send_lock, {"TASK": "QUERY", "USER": task_data["user"]})
                # Sprint 4 — tarefa entrou em execução; descarta o timestamp + antigo
                with metrics_lock:
                    metrics["tasks_running"] += 1
                with state_lock:
                    if task_timestamps:
                        task_timestamps.popleft()
                log(f"[W] QUERY entregue → {worker_uuid} "
                    f"({'emprestado' if is_borrowed else 'local'}) "
                    f"| fila restante: {task_queue.qsize()}")
            except queue.Empty:
                safe_send(conn, send_lock, {"TASK": "NO_TASK"})
                log(f"[W] NO_TASK → {worker_uuid} (fila vazia)")

        # ── Sprint 2 T03/04: Receber STATUS e enviar ACK ──────────────────────
        elif status in ("OK", "NOK") and task == "QUERY":
            w_uuid = msg.get("WORKER_UUID", worker_uuid)
            kind   = "emprestado" if is_borrowed else "local"
            log(f"[W] Worker {w_uuid} ({kind}) concluiu QUERY | STATUS={status}")
            # Sprint 4 — tarefa concluída: atualiza contadores (OK / NOK)
            with metrics_lock:
                metrics["tasks_running"] = max(0, metrics["tasks_running"] - 1)
                if status == "OK":
                    metrics["tasks_completed"] += 1
                else:
                    metrics["tasks_failed"] += 1
            safe_send(conn, send_lock, {"STATUS": "ACK", "WORKER_UUID": w_uuid})
            log(f"[W] ACK enviado → {w_uuid}")

        # ── Sprint 3 T06: Mensagem desconhecida — log + ignorar ───────────────
        else:
            log(f"[W] Mensagem desconhecida de {addr} (ignorada): {msg}")

    # Cleanup ao desconectar
    if worker_uuid:
        # Sprint 4 — conta workers_failed só se a saída NÃO foi redirect/release normal:
        #   • redirect p/ vizinho → worker ainda em `workers` e listado em lent_out
        #   • release de emprestado → já removido por saturation_monitor (pop = None)
        #   • queda inesperada     → ainda em `workers` e fora de lent_out → falha
        with state_lock:
            existed  = workers.pop(worker_uuid, None) is not None
            was_lent = worker_uuid in lent_out
        if existed and not was_lent:
            with metrics_lock:
                metrics["workers_failed"] += 1
            log(f"[W] Worker {worker_uuid} caiu inesperadamente (workers_failed++)")
        log(f"[W] Worker {worker_uuid} removido | Farm → {worker_count()}")
    log(f"[W] Conexão encerrada: {addr}")
    try:
        conn.close()
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 3 — Tarefa 03: Protocolo de Negociação (lado receptor)
# Sprint 3 — Tarefa 04: Enviar command_redirect aos Workers selecionados
# Sprint 3 — Tarefa 05: Receber notify_worker_returned
# Sprint 3 — Tarefa 06: Tipo desconhecido — log + ignorar
# ═══════════════════════════════════════════════════════════════════════════════
def handle_master(conn: socket.socket, addr: tuple) -> None:
    log(f"[M2M] Master conectado: {addr}")
    send_lock = threading.Lock()   # Sprint 3 T06 — lock por conexão

    for msg in iter_messages(conn):
        mtype      = msg.get("type", "")
        request_id = msg.get("request_id", "")
        p          = msg.get("payload", {})
        rid_short  = request_id[:8] if request_id else "?"

        # Sprint 3 T07 — loga todo recebimento M2M com type e request_id
        log(f"[M2M] ← type={mtype} request_id={rid_short}")

        # ── Sprint 3 T03: Receber request_help → avaliar e responder ──────────
        if mtype == "request_help":
            if not p.get("master_id"):
                log(f"[M2M] request_help sem master_id (ignorado)")
                continue

            master_id      = p["master_id"]
            workers_needed = p.get("workers_needed", 1)
            master_waddr   = p.get("worker_address", "")

            with state_lock:
                available = [uid for uid, w in workers.items() if not w["is_borrowed"]]

            if len(available) >= workers_needed:
                chosen  = available[:workers_needed]
                details = []
                for uid in chosen:
                    with state_lock:
                        w = workers.get(uid)
                    if w:
                        details.append({"id": uid, "address": f"{w['addr'][0]}:{w['addr'][1]}"})

                # response_accepted com mesmo request_id (Sprint 3 T03)
                safe_send(conn, send_lock, {
                    "type":       "response_accepted",
                    "request_id": request_id,
                    "payload": {
                        "workers_offered": len(chosen),
                        "worker_details":  details,
                    },
                })
                log(f"[M2M] → response_accepted | {len(chosen)} worker(s) → {master_id}")

                # ── Sprint 3 T04: Enviar command_redirect a cada worker ────────
                for uid in chosen:
                    with state_lock:
                        w = workers.get(uid)
                    if w:
                        if safe_send(w["conn"], w["send_lock"], {
                            "type":       "command_redirect",
                            "request_id": str(uuid.uuid4()),
                            "payload":    {"new_master_address": master_waddr},
                        }):
                            # Sprint 4 — registra empréstimo p/ fora (worker → master que pediu)
                            with state_lock:
                                lent_out[uid] = master_id
                            log(f"[M2M] → command_redirect → worker {uid} | destino: {master_waddr}")
            else:
                reason = "no_workers_available" if not available else "high_load"
                safe_send(conn, send_lock, {
                    "type":       "response_rejected",
                    "request_id": request_id,
                    "payload":    {"reason": reason},
                })
                log(f"[M2M] → response_rejected → {master_id} | reason={reason}")

        # ── Sprint 3 T03: Correlacionar resposta ao nosso request_help ────────
        elif mtype in ("response_accepted", "response_rejected"):
            with state_lock:
                entry = pending_m2m.get(request_id)
            if entry:
                entry["response"] = msg
                entry["event"].set()
                log(f"[M2M] Resposta correlacionada: {mtype} request_id={rid_short}")
            else:
                log(f"[M2M] Resposta sem request_id correspondente: {rid_short}")

        # ── Sprint 3 T05: Confirmar devolução do worker ───────────────────────
        elif mtype == "notify_worker_returned":
            worker_id = p.get("worker_id", "?")
            # Sprint 4 — empréstimo encerrado: sai do contador de emprestados p/ fora
            with state_lock:
                lent_out.pop(worker_id, None)
            log(f"[M2M] Worker {worker_id} devolvido. Farm atualizada.")

        # ── Sprint 3 T06: Tipo desconhecido — log + ignorar ──────────────────
        else:
            log(f"[M2M] type desconhecido '{mtype}' (ignorado)")

    log(f"[M2M] Master desconectado: {addr}")
    try:
        conn.close()
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 3 — Tarefa 03: Solicitar ajuda a Master vizinho (conexão de saída)
# ═══════════════════════════════════════════════════════════════════════════════
def request_help_from(neighbor_id: str, neighbor_info: dict, workers_needed: int) -> None:
    try:
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.settimeout(6)
        conn.connect((neighbor_info["host"], neighbor_info["master_port"]))
        send_lock = threading.Lock()

        req_id = str(uuid.uuid4())   # UUID v4 como request_id
        event  = threading.Event()
        with state_lock:
            pending_m2m[req_id] = {"event": event, "response": None}

        safe_send(conn, send_lock, {
            "type":       "request_help",
            "request_id": req_id,
            "payload": {
                "master_id":      MASTER_ID,
                "current_load":   task_queue.qsize(),
                "capacity":       CAPACITY,
                "workers_needed": workers_needed,
                "worker_address": f"{WORKER_HOST}:{WORKER_PORT}",
            },
        })
        log(f"[SAT] → request_help → {neighbor_id} | req={req_id[:8]} workers_needed={workers_needed}")

        # Lê resposta em thread separada para não bloquear saturation_monitor
        def _read():
            for resp_msg in iter_messages(conn):
                r = resp_msg.get("request_id", "")
                with state_lock:
                    if r in pending_m2m:
                        pending_m2m[r]["response"] = resp_msg
                        pending_m2m[r]["event"].set()

        threading.Thread(target=_read, daemon=True).start()

        # Sprint 3 T03 — timeout de 5s para a resposta
        if not event.wait(timeout=5):
            log(f"[SAT] Timeout aguardando {neighbor_id} (req={req_id[:8]})")
            with state_lock:
                pending_m2m.pop(req_id, None)
            conn.close()
            return

        with state_lock:
            response = pending_m2m.pop(req_id, {}).get("response")

        if response is None:
            log(f"[SAT] Sem resposta válida de {neighbor_id}")
            conn.close()
            return

        if response.get("type") == "response_accepted":
            offered = response.get("payload", {}).get("workers_offered", 0)
            log(f"[SAT] ✅ {neighbor_id} aceitou! {offered} worker(s) a caminho.")
            with state_lock:
                m2m_conns[neighbor_id] = (conn, send_lock)  # mantém para notify_worker_returned
        else:
            reason = response.get("payload", {}).get("reason", "?")
            log(f"[SAT] ❌ {neighbor_id} recusou: reason={reason}")
            conn.close()

    except Exception as e:
        log(f"[SAT] Erro ao contatar {neighbor_id}: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 3 — Tarefa 02: Detecção de Saturação e Liberação (histerese)
# Sprint 3 — Tarefa 05: Devolver Workers (command_release + notify_worker_returned)
# ═══════════════════════════════════════════════════════════════════════════════
def saturation_monitor() -> None:
    while True:
        time.sleep(5)
        load = task_queue.qsize()

        # ── Sprint 3 T05: Verificar devolução (RELEASE_THRESHOLD — histerese) ─
        with state_lock:
            borrowed_uuids = [uid for uid, w in workers.items() if w["is_borrowed"]]

        if borrowed_uuids and load < RELEASE_THRESHOLD:
            log(f"[SAT] Carga normalizada (load={load}). Devolvendo {len(borrowed_uuids)} worker(s).")
            for uid in borrowed_uuids:
                with state_lock:
                    w = workers.get(uid)
                if not w:
                    continue
                orig = w.get("original_master", "")

                # command_release ao worker emprestado
                if safe_send(w["conn"], w["send_lock"], {
                    "type":       "command_release",
                    "request_id": str(uuid.uuid4()),
                    "payload":    {"original_master_address": orig},
                }):
                    log(f"[SAT] → command_release → worker {uid}")

                # Remover worker emprestado da farm
                with state_lock:
                    workers.pop(uid, None)
                log(f"[SAT] Worker {uid} removido da farm | Farm → {worker_count()}")

                # notify_worker_returned ao Master de origem
                orig_master_id = None
                for mid, info in NEIGHBOR_MASTERS.items():
                    if orig == f"{info['host']}:{info['worker_port']}":
                        orig_master_id = mid
                        break
                if orig_master_id is None and orig in m2m_conns:
                    orig_master_id = orig

                if orig_master_id:
                    with state_lock:
                        m2m_entry = m2m_conns.get(orig_master_id)
                    if m2m_entry:
                        m2m_conn, m2m_lock = m2m_entry
                        if safe_send(m2m_conn, m2m_lock, {
                            "type":       "notify_worker_returned",
                            "request_id": str(uuid.uuid4()),
                            "payload":    {"worker_id": uid},
                        }):
                            log(f"[SAT] → notify_worker_returned → {orig_master_id} worker={uid}")
            continue

        # ── Sprint 3 T02: Verificar saturação (SATURATION_THRESHOLD) ──────────
        if load > SATURATION_THRESHOLD:
            workers_needed = max(1, (load - CAPACITY) // 3)
            log(f"[SAT] ⚠️  Saturação detectada! load={load} | pedindo {workers_needed} worker(s)")
            for nid, ninfo in NEIGHBOR_MASTERS.items():
                threading.Thread(
                    target=request_help_from,
                    args=(nid, ninfo, workers_needed),
                    daemon=True,
                ).start()


# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 2 — Tarefa 02: Simulação de Carga (populate task_queue)
# ═══════════════════════════════════════════════════════════════════════════════
def simulate_load() -> None:
    users = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry"]
    i = 0
    while True:
        time.sleep(3)
        task_queue.put({"user": users[i % len(users)]})
        with state_lock:                          # Sprint 4 — carimba a idade da tarefa
            task_timestamps.append(time.time())
        log(f"[LOAD] Tarefa adicionada (usuário={users[i % len(users)]}) | fila={task_queue.qsize()}")
        i += 1


# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 1 — Tarefa 01: Servidor TCP para Workers
# Sprint 1 — Tarefa 04: 1 thread por Worker (concorrência)
# ═══════════════════════════════════════════════════════════════════════════════
def start_worker_server() -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((WORKER_HOST, WORKER_PORT))
    srv.listen(20)
    log(f"Servidor Workers ouvindo em {WORKER_HOST}:{WORKER_PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_worker, args=(conn, addr), daemon=True).start()


# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 3 — Tarefa 01: Servidor TCP para Masters vizinhos
# Sprint 3 — Tarefa 01: 1 thread por Master vizinho (concorrência)
# ═══════════════════════════════════════════════════════════════════════════════
def start_master_server() -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((MASTER_HOST, MASTER_PORT))
    srv.listen(10)
    log(f"Servidor M2M ouvindo em {MASTER_HOST}:{MASTER_PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_master, args=(conn, addr), daemon=True).start()


# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 4 — collect_state(): snapshot consistente p/ o performance_report
#   Monta (server_uuid, hostname, start_time, farm_state, thresholds, neighbors)
#   a partir do estado já existente (workers, lent_out, task_queue, m2m_conns…).
#   Tudo lido sob os locks (state_lock / metrics_lock) para evitar leitura suja.
# ═══════════════════════════════════════════════════════════════════════════════
def collect_state():
    # Snapshot do estado da farm sob state_lock (captura tudo de uma vez)
    with state_lock:
        received_in = [(u, w["original_master"]) for u, w in workers.items() if w["is_borrowed"]]
        local_here  = [u for u, w in workers.items() if not w["is_borrowed"]]
        lent        = list(lent_out.items())
        oldest_age  = int(time.time() - task_timestamps[0]) if task_timestamps else 0
        pending     = task_queue.qsize()
        active_m2m  = set(m2m_conns.keys())
    with metrics_lock:
        m = dict(metrics)

    received_count = len(received_in)
    local_count    = len(local_here)
    alive          = received_count + local_count
    running        = m["tasks_running"]
    idle           = max(0, alive - running)

    # borrowed_workers: empréstimos PARA fora ("out") + recebidos de vizinhos ("in")
    borrowed_list  = [{"direction": "out", "peer_uuid": peer} for _, peer in lent]
    borrowed_list += [{"direction": "in",  "peer_uuid": orig} for _, orig in received_in]

    farm_state = {
        "workers": {
            "total_registered":           alive,
            "workers_utilization":        running,
            "workers_alive":              alive,
            "workers_idle":               idle,
            "workers_borrowed":           len(lent),            # emprestados PARA fora
            "workers_received":           received_count,       # recebidos de vizinhos
            "workers_failed":             m["workers_failed"],
            "workers_home":               local_count + len(lent),
            "workers_available_capacity": idle,
            "borrowed_workers":           borrowed_list,
        },
        "tasks": {
            "tasks_pending":     pending,
            "tasks_running":     running,
            "tasks_completed":   m["tasks_completed"],
            "tasks_failed":      m["tasks_failed"],
            "oldest_task_age_s": oldest_age,
        },
    }

    thresholds = {
        "max_task":            CAPACITY,
        "warn_cpu_percent":    supervisor.WARN_CPU,
        "warn_memory_percent": supervisor.WARN_MEMORY,
        "release_task":        RELEASE_THRESHOLD,
    }

    now_iso   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    neighbors = [
        {"server_uuid":    nid,
         "status":         "available" if nid in active_m2m else "unavailable",
         "last_heartbeat": now_iso}
        for nid in NEIGHBOR_MASTERS
    ]

    return (SERVER_UUID, HOSTNAME, START_TIME, farm_state, thresholds, neighbors)


# ═══════════════════════════════════════════════════════════════════════════════
# Entry Point — Arquitetura de threads (SPEC §4)
#
#   main thread  →  start_worker_server()   (bloqueia — Sprint 1 T01)
#   daemon       →  start_master_server()   (Sprint 3 T01)
#   daemon       →  simulate_load()         (Sprint 2 T02)
#   daemon       →  saturation_monitor()    (Sprint 3 T02)
#   daemon       →  supervisor reporter      (Sprint 4 — métricas a cada 10s)
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log(f"Iniciando Master {MASTER_ID}")
    log(f"  Worker port : {WORKER_PORT}")
    log(f"  M2M port    : {MASTER_PORT}")
    log(f"  Threshold   : saturação={SATURATION_THRESHOLD} liberação={RELEASE_THRESHOLD}")
    log(f"  Vizinhos    : {list(NEIGHBOR_MASTERS.keys()) or 'nenhum configurado'}")

    threading.Thread(target=simulate_load,       daemon=True).start()
    threading.Thread(target=saturation_monitor,  daemon=True).start()
    threading.Thread(target=start_master_server, daemon=True).start()

    # Sprint 4 — reporter de métricas ao Supervisor do cluster (TLS, a cada 10s)
    supervisor.start_reporter(collect_state)
    log(f"Reporter de métricas (Sprint 4) iniciado → {supervisor.SUP_HOST}:{supervisor.SUP_PORT} a cada {supervisor.REPORT_INTERVAL}s")

    start_worker_server()
