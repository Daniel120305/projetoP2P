import socket
import threading
import json
import time
import uuid
import queue
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# Configuração
# ─────────────────────────────────────────────────────────────────────────────
MASTER_ID   = "Master_A"
WORKER_HOST = "10.62.206.13"
WORKER_PORT = 10000          # Workers conectam aqui (Sprint 1 e 2)
MASTER_HOST = "10.62.206.13"
MASTER_PORT = 10001          # Masters vizinhos conectam aqui (Sprint 3)

# Thresholds com histerese (Sprint 3 T02)
CAPACITY             = 10   # capacidade nominal da fila
SATURATION_THRESHOLD = 10   # load > este → solicitar ajuda
RELEASE_THRESHOLD    = 4    # load < este → devolver workers

# Diretório de Masters vizinhos: {"Master_B": {"host": "...", "master_port": 5011, "worker_port": 5010}}
NEIGHBOR_MASTERS: dict = {
    # Exemplo:
    # "Master_B": {"host": "127.0.0.1", "master_port": 5011, "worker_port": 5010}
}

# ─────────────────────────────────────────────────────────────────────────────
# Estado compartilhado
# ─────────────────────────────────────────────────────────────────────────────
task_queue  = queue.Queue()   # fila de tarefas pendentes (thread-safe)
state_lock  = threading.Lock()

# workers: {worker_uuid: {"conn", "addr", "is_borrowed", "original_master", "send_lock"}}
workers: dict = {}

# pending_m2m: {request_id: {"event": Event, "response": dict|None}}
pending_m2m: dict = {}

# conexões M2M abertas: {master_id: (conn, send_lock)} — reutilizadas p/ notify_worker_returned
m2m_conns: dict = {}

# ─────────────────────────────────────────────────────────────────────────────
# Utilitários
# ─────────────────────────────────────────────────────────────────────────────
def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [MASTER {MASTER_ID}] {msg}", flush=True)


def safe_send(conn: socket.socket, send_lock: threading.Lock, data: dict) -> bool:
    """Envia JSON + '\\n' de forma thread-safe."""
    with send_lock:
        try:
            conn.send((json.dumps(data) + "\n").encode("utf-8"))
            return True
        except Exception as e:
            log(f"Erro ao enviar: {e}")
            return False


def iter_messages(conn: socket.socket):
    """Generator: lê linhas JSON de uma conexão persistente."""
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
    """Retorna resumo de workers locais vs emprestados."""
    with state_lock:
        local    = sum(1 for w in workers.values() if not w["is_borrowed"])
        borrowed = sum(1 for w in workers.values() if w["is_borrowed"])
    return f"locais={local} emprestados={borrowed} total={local+borrowed}"


# ─────────────────────────────────────────────────────────────────────────────
# Sprint 1 + 2 + 3-registro: handler de Workers
# ─────────────────────────────────────────────────────────────────────────────
def handle_worker(conn: socket.socket, addr: tuple) -> None:
    """
    Trata uma conexão de Worker.
    Cobre Sprint 1 (HEARTBEAT), Sprint 2 (ALIVE/QUERY/STATUS/ACK)
    e Sprint 3 (register_temporary_worker).
    """
    log(f"[W] Nova conexão: {addr}")
    send_lock   = threading.Lock()
    worker_uuid = None
    is_borrowed = False

    for msg in iter_messages(conn):
        task    = msg.get("TASK", "")
        worker  = msg.get("WORKER", "")
        status  = msg.get("STATUS", "")
        mtype   = msg.get("type", "")

        # ── Sprint 1: Heartbeat ───────────────────────────────────────────────
        if task == "HEARTBEAT":
            # Campo obrigatório: SERVER_UUID (qual master o worker está verificando)
            resp = {
                "SERVER_UUID": MASTER_ID,
                "TASK":        "HEARTBEAT",
                "RESPONSE":    "ALIVE"
            }
            safe_send(conn, send_lock, resp)
            log(f"[W] HEARTBEAT respondido → {addr}")

        # ── Sprint 3: Worker temporário se registra ───────────────────────────
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

        # ── Sprint 2 T01: Apresentação / Pedido de tarefa ─────────────────────
        elif worker == "ALIVE":
            worker_uuid = msg.get("WORKER_UUID")
            server_uuid = msg.get("SERVER_UUID")   # presente somente se emprestado

            if not worker_uuid:
                log(f"[W] ALIVE sem WORKER_UUID ignorado: {msg}")
                continue

            if server_uuid:                         # worker emprestado
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
            else:                                   # worker local
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

            # Sprint 2 T02: entregar tarefa ou informar fila vazia
            try:
                task_data = task_queue.get_nowait()
                resp = {"TASK": "QUERY", "USER": task_data["user"]}
                safe_send(conn, send_lock, resp)
                log(f"[W] QUERY entregue → {worker_uuid} "
                    f"({'emprestado' if is_borrowed else 'local'}) "
                    f"| fila restante: {task_queue.qsize()}")
            except queue.Empty:
                safe_send(conn, send_lock, {"TASK": "NO_TASK"})
                log(f"[W] NO_TASK → {worker_uuid} (fila vazia)")

        # ── Sprint 2 T03/04: Reporte de status e ACK ─────────────────────────
        elif status in ("OK", "NOK") and task == "QUERY":
            w_uuid = msg.get("WORKER_UUID", worker_uuid)
            kind   = "emprestado" if is_borrowed else "local"
            log(f"[W] Worker {w_uuid} ({kind}) concluiu QUERY | STATUS={status}")
            safe_send(conn, send_lock, {"STATUS": "ACK", "WORKER_UUID": w_uuid})
            log(f"[W] ACK enviado → {w_uuid}")

        # ── Mensagem desconhecida: ignorar com log (compatibilidade futura) ───
        else:
            log(f"[W] Mensagem desconhecida de {addr} (ignorada): {msg}")

    # ── Cleanup ao desconectar ────────────────────────────────────────────────
    if worker_uuid:
        with state_lock:
            workers.pop(worker_uuid, None)
        log(f"[W] Worker {worker_uuid} removido | Farm → {worker_count()}")
    log(f"[W] Conexão encerrada: {addr}")
    try:
        conn.close()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Sprint 3: handler de Masters vizinhos (servidor M2M)
# ─────────────────────────────────────────────────────────────────────────────
def handle_master(conn: socket.socket, addr: tuple) -> None:
    """
    Trata uma conexão Master-to-Master.
    Sprint 3 T01 (infra) + T03 (negociação) + T04 (redirect) + T05 (devolução).
    """
    log(f"[M2M] Master conectado: {addr}")
    send_lock = threading.Lock()

    for msg in iter_messages(conn):
        mtype      = msg.get("type", "")
        request_id = msg.get("request_id", "")
        p          = msg.get("payload", {})
        rid_short  = request_id[:8] if request_id else "?"

        log(f"[M2M] ← type={mtype} request_id={rid_short}")

        # ── Sprint 3 T03: Receber pedido de ajuda ────────────────────────────
        if mtype == "request_help":
            if not p.get("master_id"):
                log(f"[M2M] request_help sem master_id (ignorado)")
                continue

            master_id       = p["master_id"]
            workers_needed  = p.get("workers_needed", 1)
            master_waddr    = p.get("worker_address", "")   # endereço worker-port do Master A

            with state_lock:
                available = [uid for uid, w in workers.items() if not w["is_borrowed"]]

            if len(available) >= workers_needed:
                chosen  = available[:workers_needed]
                details = []
                for uid in chosen:
                    with state_lock:
                        w = workers.get(uid)
                    if w:
                        details.append({
                            "id":      uid,
                            "address": f"{w['addr'][0]}:{w['addr'][1]}"
                        })

                resp = {
                    "type":       "response_accepted",
                    "request_id": request_id,       # mesmo request_id da requisição
                    "payload": {
                        "workers_offered": len(chosen),
                        "worker_details":  details,
                    },
                }
                safe_send(conn, send_lock, resp)
                log(f"[M2M] → response_accepted | {len(chosen)} worker(s) → {master_id}")

                # Sprint 3 T04: enviar command_redirect a cada worker selecionado
                for uid in chosen:
                    with state_lock:
                        w = workers.get(uid)
                    if w:
                        cmd = {
                            "type":       "command_redirect",
                            "request_id": str(uuid.uuid4()),
                            "payload":    {"new_master_address": master_waddr},
                        }
                        if safe_send(w["conn"], w["send_lock"], cmd):
                            log(f"[M2M] → command_redirect → worker {uid} | destino: {master_waddr}")
            else:
                reason = "no_workers_available" if not available else "high_load"
                resp = {
                    "type":       "response_rejected",
                    "request_id": request_id,
                    "payload":    {"reason": reason},
                }
                safe_send(conn, send_lock, resp)
                log(f"[M2M] → response_rejected → {master_id} | reason={reason}")

        # ── Respostas ao nosso request_help (chegam aqui via conexão de saída) ─
        elif mtype in ("response_accepted", "response_rejected"):
            with state_lock:
                entry = pending_m2m.get(request_id)
            if entry:
                entry["response"] = msg
                entry["event"].set()
                log(f"[M2M] Resposta correlacionada: {mtype} request_id={rid_short}")
            else:
                log(f"[M2M] Resposta sem request_id correspondente: {rid_short}")

        # ── Sprint 3 T05: Worker devolvido ────────────────────────────────────
        elif mtype == "notify_worker_returned":
            worker_id = p.get("worker_id", "?")
            log(f"[M2M] Worker {worker_id} devolvido. Farm atualizada.")

        # ── Tipo desconhecido ─────────────────────────────────────────────────
        else:
            log(f"[M2M] type desconhecido '{mtype}' (ignorado)")

    log(f"[M2M] Master desconectado: {addr}")
    try:
        conn.close()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Sprint 3 T03: solicitar ajuda a um Master vizinho (conexão de saída)
# ─────────────────────────────────────────────────────────────────────────────
def request_help_from(neighbor_id: str, neighbor_info: dict, workers_needed: int) -> None:
    """Abre conexão com Master vizinho e realiza protocolo de negociação."""
    try:
        h   = neighbor_info["host"]
        mp  = neighbor_info["master_port"]
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.settimeout(6)
        conn.connect((h, mp))
        send_lock = threading.Lock()

        req_id = str(uuid.uuid4())
        event  = threading.Event()
        with state_lock:
            pending_m2m[req_id] = {"event": event, "response": None}

        msg = {
            "type":       "request_help",
            "request_id": req_id,
            "payload": {
                "master_id":      MASTER_ID,
                "current_load":   task_queue.qsize(),
                "capacity":       CAPACITY,
                "workers_needed": workers_needed,
                "worker_address": f"{WORKER_HOST}:{WORKER_PORT}",
            },
        }
        safe_send(conn, send_lock, msg)
        log(f"[SAT] → request_help → {neighbor_id} | req={req_id[:8]} workers_needed={workers_needed}")

        # Ler respostas em thread separada para não bloquear o saturation_monitor
        def _read():
            for resp_msg in iter_messages(conn):
                r = resp_msg.get("request_id", "")
                with state_lock:
                    if r in pending_m2m:
                        pending_m2m[r]["response"] = resp_msg
                        pending_m2m[r]["event"].set()

        t = threading.Thread(target=_read, daemon=True)
        t.start()

        # Aguardar resposta até 5 segundos (Sprint 3 T03)
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

        rtype = response.get("type", "")
        if rtype == "response_accepted":
            offered = response.get("payload", {}).get("workers_offered", 0)
            log(f"[SAT] ✅ {neighbor_id} aceitou! {offered} worker(s) a caminho.")
            # Manter conexão aberta para notify_worker_returned posterior
            with state_lock:
                m2m_conns[neighbor_id] = (conn, send_lock)
        else:
            reason = response.get("payload", {}).get("reason", "?")
            log(f"[SAT] ❌ {neighbor_id} recusou: reason={reason}")
            conn.close()

    except Exception as e:
        log(f"[SAT] Erro ao contatar {neighbor_id}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Sprint 3 T02: monitor de saturação e liberação
# ─────────────────────────────────────────────────────────────────────────────
def saturation_monitor() -> None:
    """
    Roda a cada 5s.
    - Se load > SATURATION_THRESHOLD → solicitar ajuda aos vizinhos.
    - Se load < RELEASE_THRESHOLD e há workers emprestados → devolvê-los.
    """
    while True:
        time.sleep(5)
        load = task_queue.qsize()

        # ── Verificar se deve devolver workers ───────────────────────────────
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

                # Sprint 3 T05: command_release ao worker
                cmd_release = {
                    "type":       "command_release",
                    "request_id": str(uuid.uuid4()),
                    "payload":    {"original_master_address": orig},
                }
                if safe_send(w["conn"], w["send_lock"], cmd_release):
                    log(f"[SAT] → command_release → worker {uid}")

                # Remover worker emprestado da farm imediatamente
                with state_lock:
                    workers.pop(uid, None)
                log(f"[SAT] Worker {uid} removido da farm | Farm → {worker_count()}")

                # Sprint 3 T05: notify_worker_returned ao Master de origem
                # Cruzar endereço original com NEIGHBOR_MASTERS para achar o master_id correto
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
                        notif = {
                            "type":       "notify_worker_returned",
                            "request_id": str(uuid.uuid4()),
                            "payload":    {"worker_id": uid},
                        }
                        if safe_send(m2m_conn, m2m_lock, notif):
                            log(f"[SAT] → notify_worker_returned → {orig_master_id} worker={uid}")
            continue  # o loop continuará na próxima iteração do while True

        # ── Verificar saturação ───────────────────────────────────────────────
        if load > SATURATION_THRESHOLD:
            workers_needed = max(1, (load - CAPACITY) // 3)
            log(f"[SAT] ⚠️  Saturação detectada! load={load} | pedindo {workers_needed} worker(s)")
            for nid, ninfo in NEIGHBOR_MASTERS.items():
                threading.Thread(
                    target=request_help_from,
                    args=(nid, ninfo, workers_needed),
                    daemon=True
                ).start()


# ─────────────────────────────────────────────────────────────────────────────
# Simulação de carga (Sprint 2 T02)
# ─────────────────────────────────────────────────────────────────────────────
def simulate_load() -> None:
    """Adiciona tarefas à fila periodicamente para simular chegada de requisições."""
    users = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry"]
    i = 0
    while True:
        time.sleep(3)
        task_queue.put({"user": users[i % len(users)]})
        log(f"[LOAD] Tarefa adicionada (usuário={users[i % len(users)]}) | fila={task_queue.qsize()}")
        i += 1


# ─────────────────────────────────────────────────────────────────────────────
# Servidores TCP
# ─────────────────────────────────────────────────────────────────────────────
def start_worker_server() -> None:
    """Sprint 1 T01: Servidor para conexões de Workers."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((WORKER_HOST, WORKER_PORT))
    srv.listen(20)
    log(f"Servidor Workers ouvindo em {WORKER_HOST}:{WORKER_PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_worker, args=(conn, addr), daemon=True).start()


def start_master_server() -> None:
    """Sprint 3 T01: Servidor para conexões Master-to-Master."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((MASTER_HOST, MASTER_PORT))
    srv.listen(10)
    log(f"Servidor M2M ouvindo em {MASTER_HOST}:{MASTER_PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_master, args=(conn, addr), daemon=True).start()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log(f"Iniciando Master {MASTER_ID}")
    log(f"  Worker port : {WORKER_PORT}")
    log(f"  M2M port    : {MASTER_PORT}")
    log(f"  Threshold   : saturação={SATURATION_THRESHOLD} liberação={RELEASE_THRESHOLD}")
    log(f"  Vizinhos    : {list(NEIGHBOR_MASTERS.keys()) or 'nenhum configurado'}")

    threading.Thread(target=simulate_load,       daemon=True).start()
    threading.Thread(target=saturation_monitor,  daemon=True).start()
    threading.Thread(target=start_master_server, daemon=True).start()
    start_worker_server()   # bloqueia o processo principal
