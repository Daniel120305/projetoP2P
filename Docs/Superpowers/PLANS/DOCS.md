# Documentação — Código por Código

**Projeto:** projetoP2P  
**Arquivos:** `master.py` · `worker.py`  
**Framework:** PLAN.md + SPEC.md  

---

## Sprint 1 — Heartbeat (Infraestrutura TCP)

---

### Tarefa 01 — Infraestrutura TCP

**O que o PLAN pede:**
> Master escuta em porta definida · Worker conecta como cliente · Delimitador `\n` nos dois lados

**master.py — configuração da porta:**
```python
WORKER_HOST = "10.62.206.13"
WORKER_PORT = 10000          # Workers conectam aqui
```

**master.py — servidor que aceita Workers:**
```python
def start_worker_server() -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((WORKER_HOST, WORKER_PORT))
    srv.listen(20)
    log(f"Servidor Workers ouvindo em {WORKER_HOST}:{WORKER_PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_worker, args=(conn, addr), daemon=True).start()
```

**master.py — delimitador `\n` (parser de stream TCP):**
```python
def iter_messages(conn: socket.socket):
    buffer = ""
    while True:
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
```

**worker.py — conexão ao Master:**
```python
MASTER_HOST = "10.62.206.13"
MASTER_PORT = 10000
```

**worker.py — delimitador `\n` no envio:**
```python
def _send(self, data: dict) -> bool:
    with self.send_lock:
        self.conn.send((json.dumps(data) + "\n").encode("utf-8"))
```

---

### Tarefa 02 — Lógica de Requisição (Worker → Master)

**O que o PLAN pede:**
> Worker envia `{"SERVER_UUID": "<master_id>", "TASK": "HEARTBEAT"}` · Heartbeat periódico a cada 30s em thread separada · Log: `"Heartbeat enviado para <master_id>"`

**worker.py — intervalo configurável:**
```python
HEARTBEAT_INTERVAL = 30   # segundos entre heartbeats
```

**worker.py — thread de heartbeat periódico:**
```python
def _heartbeat_loop(self) -> None:
    while self.active:
        time.sleep(HEARTBEAT_INTERVAL)
        if not self.active:
            break
        if self._send({"SERVER_UUID": self.master_id, "TASK": "HEARTBEAT"}):
            log(f"Heartbeat enviado para {self.master_id}")
        else:
            log("Status: OFFLINE - Tentando Reconectar")
            self.active = False
            break
```

**worker.py — thread iniciada em `run()`:**
```python
threading.Thread(target=self._heartbeat_loop, daemon=True).start()
```

---

### Tarefa 03 — Lógica de Resposta (Master → Worker)

**O que o PLAN pede:**
> Master identifica `TASK == "HEARTBEAT"` · Resposta: `{"SERVER_UUID": "<master_id>", "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}` · Log do worker: `"Status: ALIVE"` ou `"Status: OFFLINE - Tentando Reconectar"`

**master.py — identificação e resposta HEARTBEAT:**
```python
if task == "HEARTBEAT":
    safe_send(conn, send_lock, {
        "SERVER_UUID": MASTER_ID,
        "TASK":        "HEARTBEAT",
        "RESPONSE":    "ALIVE",
    })
    log(f"[W] HEARTBEAT respondido → {addr}")
```

**worker.py — log inline na thread leitora:**
```python
elif msg.get("TASK") == "HEARTBEAT" and msg.get("RESPONSE") == "ALIVE":
    log("Status: ALIVE")
```

**worker.py — log de falha na `_heartbeat_loop`:**
```python
log("Status: OFFLINE - Tentando Reconectar")
```

---

### Tarefa 04 — Concorrência e Resiliência

**O que o PLAN pede:**
> Master usa threads por worker · Worker usa thread separada para heartbeat · Worker reconecta automaticamente com loop + sleep(5)

**master.py — 1 thread por Worker:**
```python
while True:
    conn, addr = srv.accept()
    threading.Thread(target=handle_worker, args=(conn, addr), daemon=True).start()
```

**worker.py — reconexão automática com loop:**
```python
def _connect(self) -> None:
    while True:
        try:
            self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.conn.connect((self.host, self.port))
            self.active = True
            log(f"Conectado ao Master {self.master_id} ({self.host}:{self.port})")
            return
        except Exception as e:
            log(f"Falha ao conectar: {e}. Tentando novamente em 5s…")
            time.sleep(5)
```

**worker.py — thread leitora separada (não bloqueia ciclo de tarefa):**
```python
threading.Thread(target=self._reader_thread, daemon=True).start()
```

---

## Sprint 2 — Ciclo de Tarefas

---

### Tarefa 01 — Apresentação e Identificação (Worker → Master)

**O que o PLAN pede:**
> Worker gera WORKER_UUID único · Envia `{"WORKER": "ALIVE", "WORKER_UUID": "..."}` · Se emprestado, inclui `"SERVER_UUID"` · Master registra worker no dicionário `workers`

**worker.py — UUID único gerado na inicialização:**
```python
WORKER_UUID = str(uuid.uuid4())
```

**worker.py — envio do ALIVE em `_task_cycle()`:**
```python
alive: dict = {"WORKER": "ALIVE", "WORKER_UUID": WORKER_UUID}
if self.is_borrowed:
    alive["SERVER_UUID"] = self.orig_master_id   # identifica master de origem
self._send(alive)
log(f"ALIVE enviado "
    f"{'(emprestado de ' + self.orig_master_id + ')' if self.is_borrowed else '(local)'}")
```

**master.py — registro no dicionário `workers`:**
```python
elif worker == "ALIVE":
    worker_uuid = msg.get("WORKER_UUID")
    server_uuid = msg.get("SERVER_UUID")   # presente somente se emprestado

    if server_uuid:                        # worker emprestado
        is_borrowed = True
        with state_lock:
            workers[worker_uuid] = {
                "conn":            conn,
                "addr":            addr,
                "is_borrowed":     True,
                "original_master": server_uuid,
                "send_lock":       send_lock,
            }
    else:                                  # worker local
        is_borrowed = False
        with state_lock:
            workers[worker_uuid] = {
                "conn":            conn,
                "addr":            addr,
                "is_borrowed":     False,
                "original_master": None,
                "send_lock":       send_lock,
            }
```

---

### Tarefa 02 — Distribuição de Carga (Master → Worker)

**O que o PLAN pede:**
> `task_queue = queue.Queue()` · `simulate_load()` popula a fila · Master responde `{"TASK": "QUERY", "USER": "..."}` ou `{"TASK": "NO_TASK"}`

**master.py — fila thread-safe:**
```python
task_queue = queue.Queue()
```

**master.py — `simulate_load()` adiciona tarefas a cada 3s:**
```python
def simulate_load() -> None:
    users = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry"]
    i = 0
    while True:
        time.sleep(3)
        task_queue.put({"user": users[i % len(users)]})
        log(f"[LOAD] Tarefa adicionada (usuário={users[i % len(users)]}) | fila={task_queue.qsize()}")
        i += 1
```

**master.py — entrega QUERY ou NO_TASK após ALIVE:**
```python
try:
    task_data = task_queue.get_nowait()
    safe_send(conn, send_lock, {"TASK": "QUERY", "USER": task_data["user"]})
    log(f"[W] QUERY entregue → {worker_uuid} ({'emprestado' if is_borrowed else 'local'}) "
        f"| fila restante: {task_queue.qsize()}")
except queue.Empty:
    safe_send(conn, send_lock, {"TASK": "NO_TASK"})
    log(f"[W] NO_TASK → {worker_uuid} (fila vazia)")
```

---

### Tarefa 03 — Simulação de Processamento (Worker → Master)

**O que o PLAN pede:**
> Worker simula processamento com `time.sleep(random.uniform(1, 3))` · Envia `{"STATUS": "OK|NOK", "TASK": "QUERY", "WORKER_UUID": "..."}` · Log: tarefa concluída com status

**worker.py — processamento e envio do STATUS:**
```python
if task == "QUERY":
    user = resp.get("USER", "desconhecido")
    log(f"Processando QUERY para usuário: {user}")
    time.sleep(random.uniform(1, 3))

    status = "OK" if random.random() > 0.1 else "NOK"   # 90 % OK, 10 % NOK
    self._send({"STATUS": status, "TASK": "QUERY", "WORKER_UUID": WORKER_UUID})
    log(f"STATUS {status} enviado")
```

---

### Tarefa 04 — ACK e Persistência (Master → Worker)

**O que o PLAN pede:**
> Master envia `{"STATUS": "ACK", "WORKER_UUID": "..."}` · Master loga worker local ou emprestado · Worker recebe ACK e fecha o ciclo

**master.py — recebe STATUS e envia ACK:**
```python
elif status in ("OK", "NOK") and task == "QUERY":
    w_uuid = msg.get("WORKER_UUID", worker_uuid)
    kind   = "emprestado" if is_borrowed else "local"
    log(f"[W] Worker {w_uuid} ({kind}) concluiu QUERY | STATUS={status}")
    safe_send(conn, send_lock, {"STATUS": "ACK", "WORKER_UUID": w_uuid})
    log(f"[W] ACK enviado → {w_uuid}")
```

**worker.py — aguarda ACK com timeout de 5s:**
```python
try:
    ack = self.response_q.get(timeout=5)
    if ack and ack.get("STATUS") == "ACK":
        log(f"ACK recebido ✓ | ciclo de tarefa concluído (status={status})")
    else:
        log(f"ACK inesperado ou timeout: {ack}")
except queue.Empty:
    log("Timeout aguardando ACK")
```

---

## Sprint 3 — Protocolo Master-to-Master

---

### Tarefa 01 — Conexão TCP entre Masters

**O que o PLAN pede:**
> Master escuta segunda porta · Diretório de vizinhos `NEIGHBOR_MASTERS` · Delimitador `\n` para M2M · `handle_master()` em thread separada

**master.py — porta M2M separada da porta de Workers:**
```python
MASTER_HOST = "10.62.206.13"
MASTER_PORT = 10001          # porta diferente de WORKER_PORT (10000)
```

**master.py — diretório de vizinhos:**
```python
NEIGHBOR_MASTERS: dict = {
    # "Master_B": {"host": "10.x.x.x", "master_port": 10011, "worker_port": 10010}
}
```

**master.py — servidor M2M com 1 thread por vizinho:**
```python
def start_master_server() -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((MASTER_HOST, MASTER_PORT))
    srv.listen(10)
    log(f"Servidor M2M ouvindo em {MASTER_HOST}:{MASTER_PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_master, args=(conn, addr), daemon=True).start()
```

---

### Tarefa 02 — Detecção de Saturação

**O que o PLAN pede:**
> `SATURATION_THRESHOLD = 10`, `RELEASE_THRESHOLD = 4` · `saturation_monitor()` a cada 5s · Detecta saturação e inicia devolução

**master.py — thresholds com histerese:**
```python
CAPACITY             = 10   # capacidade nominal da fila
SATURATION_THRESHOLD = 10   # load > este → solicitar ajuda
RELEASE_THRESHOLD    = 4    # load < este → devolver workers
```

**master.py — `saturation_monitor()` loop a cada 5s:**
```python
def saturation_monitor() -> None:
    while True:
        time.sleep(5)
        load = task_queue.qsize()

        # Verificar devolução primeiro (histerese — evita ping-pong)
        with state_lock:
            borrowed_uuids = [uid for uid, w in workers.items() if w["is_borrowed"]]

        if borrowed_uuids and load < RELEASE_THRESHOLD:
            # ... inicia devolução (Tarefa 05)
            continue

        # Verificar saturação
        if load > SATURATION_THRESHOLD:
            workers_needed = max(1, (load - CAPACITY) // 3)
            log(f"[SAT] ⚠️  Saturação detectada! load={load} | pedindo {workers_needed} worker(s)")
            for nid, ninfo in NEIGHBOR_MASTERS.items():
                threading.Thread(
                    target=request_help_from,
                    args=(nid, ninfo, workers_needed),
                    daemon=True,
                ).start()
```

---

### Tarefa 03 — Protocolo de Negociação

**O que o PLAN pede:**
> `request_help` com UUID v4 · `pending_m2m` com `threading.Event` · Timeout de 5s · Master vizinho responde `response_accepted` ou `response_rejected` com mesmo `request_id`

**master.py — emitir `request_help` com UUID v4:**
```python
def request_help_from(neighbor_id: str, neighbor_info: dict, workers_needed: int) -> None:
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
```

**master.py — timeout de 5s aguardando resposta:**
```python
if not event.wait(timeout=5):
    log(f"[SAT] Timeout aguardando {neighbor_id} (req={req_id[:8]})")
    with state_lock:
        pending_m2m.pop(req_id, None)
    conn.close()
    return
```

**master.py — lado receptor: avalia workers e responde com mesmo `request_id`:**
```python
if mtype == "request_help":
    if len(available) >= workers_needed:
        safe_send(conn, send_lock, {
            "type":       "response_accepted",
            "request_id": request_id,     # mesmo request_id da requisição
            "payload": {
                "workers_offered": len(chosen),
                "worker_details":  details,
            },
        })
    else:
        reason = "no_workers_available" if not available else "high_load"
        safe_send(conn, send_lock, {
            "type":       "response_rejected",
            "request_id": request_id,
            "payload":    {"reason": reason},
        })
```

**master.py — correlação da resposta pelo `request_id`:**
```python
elif mtype in ("response_accepted", "response_rejected"):
    with state_lock:
        entry = pending_m2m.get(request_id)
    if entry:
        entry["response"] = msg
        entry["event"].set()
```

---

### Tarefa 04 — Redirecionamento de Workers

**O que o PLAN pede:**
> Master envia `command_redirect` · Worker trata via `command_q` · Worker conecta ao novo Master e envia `register_temporary_worker` · Master registra worker como emprestado

**master.py — enviar `command_redirect` a cada worker selecionado:**
```python
for uid in chosen:
    with state_lock:
        w = workers.get(uid)
    if w:
        if safe_send(w["conn"], w["send_lock"], {
            "type":       "command_redirect",
            "request_id": str(uuid.uuid4()),
            "payload":    {"new_master_address": master_waddr},
        }):
            log(f"[M2M] → command_redirect → worker {uid} | destino: {master_waddr}")
```

**worker.py — thread leitora roteia `command_redirect` para `command_q`:**
```python
if mtype in ("command_redirect", "command_release"):
    self.command_q.put(msg)
```

**worker.py — `_task_cycle()` verifica `command_q` antes de pedir tarefa:**
```python
try:
    return self._handle_command(self.command_q.get_nowait())
except queue.Empty:
    pass
```

**worker.py — tratamento de `command_redirect`:**
```python
if mtype == "command_redirect":
    addr = p.get("new_master_address", "")
    log(f"command_redirect recebido → novo Master: {addr}")
    return ("redirect", addr)
```

**worker.py — `main()` conecta ao novo Master como emprestado:**
```python
if action == "redirect":
    log(f"Redirecionando para: {addr}")
    if ":" in addr:
        h, p = addr.rsplit(":", 1)
        host, port = h, int(p)
    master_id   = addr
    is_borrowed = True
```

**worker.py — envia `register_temporary_worker` ao novo Master:**
```python
def _register_temporary(self) -> None:
    if self._send({
        "type":       "register_temporary_worker",
        "request_id": str(uuid.uuid4()),
        "payload": {
            "worker_id":               WORKER_UUID,
            "original_master_address": f"{self.orig_host}:{self.orig_port}",
        },
    }):
        log(f"register_temporary_worker enviado | origem: {self.orig_host}:{self.orig_port}")
```

**master.py — registra worker emprestado:**
```python
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
```

---

### Tarefa 05 — Devolução do Worker

**O que o PLAN pede:**
> `saturation_monitor` detecta `load < RELEASE_THRESHOLD` · Envia `command_release` · Envia `notify_worker_returned` ao Master de origem · Worker retorna ao Master original

**master.py — detecta carga normalizada e inicia devolução:**
```python
if borrowed_uuids and load < RELEASE_THRESHOLD:
    log(f"[SAT] Carga normalizada (load={load}). Devolvendo {len(borrowed_uuids)} worker(s).")
    for uid in borrowed_uuids:
        ...
        if safe_send(w["conn"], w["send_lock"], {
            "type":       "command_release",
            "request_id": str(uuid.uuid4()),
            "payload":    {"original_master_address": orig},
        }):
            log(f"[SAT] → command_release → worker {uid}")

        with state_lock:
            workers.pop(uid, None)
        log(f"[SAT] Worker {uid} removido da farm | Farm → {worker_count()}")
```

**master.py — envia `notify_worker_returned` ao Master de origem:**
```python
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
```

**worker.py — trata `command_release` e retorna ao Master original:**
```python
if mtype == "command_release":
    addr = p.get("original_master_address", "")
    log(f"command_release recebido → retornando para: {addr}")
    return ("release", addr)
```

**worker.py — `main()` restaura estado original:**
```python
elif action == "release":
    log(f"Liberado. Retornando ao Master original: {orig_host}:{orig_port}")
    host        = orig_host
    port        = orig_port
    master_id   = orig_id
    is_borrowed = False
```

---

### Tarefa 06 — Concorrência e Resiliência

**O que o PLAN pede:**
> `send_lock` por conexão · `state_lock` protege dicionários · Desconexão inesperada → reconectar · Mensagem desconhecida → log + ignorar

**master.py — `send_lock` por conexão no Master:**
```python
send_lock = threading.Lock()   # criado em handle_worker e handle_master

def safe_send(conn, send_lock, data):
    with send_lock:             # garante que apenas 1 thread escreve por vez
        conn.send((json.dumps(data) + "\n").encode("utf-8"))
```

**master.py — `state_lock` protege o dicionário `workers`:**
```python
state_lock = threading.Lock()

with state_lock:
    workers[worker_uuid] = { ... }
```

**worker.py — `send_lock` no Worker:**
```python
self.send_lock: threading.Lock = threading.Lock()

def _send(self, data: dict) -> bool:
    with self.send_lock:
        self.conn.send((json.dumps(data) + "\n").encode("utf-8"))
```

**master.py — mensagem desconhecida → log + ignorar (não derruba o processo):**
```python
else:
    log(f"[W] Mensagem desconhecida de {addr} (ignorada): {msg}")

# No handle_master:
else:
    log(f"[M2M] type desconhecido '{mtype}' (ignorado)")
```

**worker.py — desconexão inesperada → reconectar ao mesmo host (Tarefa 04 reaproveitada):**
```python
else:
    log("Conexão encerrada. Reconectando em 5s…")
    time.sleep(5)
    # loop while True em main() cria novo WorkerClient com mesmo host/port
```

---

### Tarefa 07 — Logs e Observabilidade

**O que o PLAN pede:**
> Todo envio/recebimento M2M loga `type`, `request_id[:8]`, timestamp · Contador locais vs emprestados a cada mudança · Ciclo de vida completo do worker emprestado

**master.py — log com timestamp em toda mensagem M2M:**
```python
def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [MASTER {MASTER_ID}] {msg}", flush=True)

# Em handle_master — todo recebimento:
log(f"[M2M] ← type={mtype} request_id={rid_short}")

# Todo envio M2M inclui log equivalente:
log(f"[M2M] → response_accepted | {len(chosen)} worker(s) → {master_id}")
log(f"[M2M] → command_redirect → worker {uid} | destino: {master_waddr}")
log(f"[SAT] → notify_worker_returned → {orig_master_id} worker={uid}")
```

**master.py — contador locais vs emprestados exibido a cada mudança:**
```python
def worker_count() -> str:
    with state_lock:
        local    = sum(1 for w in workers.values() if not w["is_borrowed"])
        borrowed = sum(1 for w in workers.values() if w["is_borrowed"])
    return f"locais={local} emprestados={borrowed} total={local+borrowed}"

# Chamado após cada entrada/saída de worker:
log(f"[W] Farm → {worker_count()}")
```

**Ciclo de vida completo de um worker emprestado nos logs:**
```
[M2M] ← type=request_help request_id=a1b2c3d4
[M2M] → response_accepted | 2 worker(s) → Master_A
[M2M] → command_redirect → worker <uuid> | destino: 10.62.206.13:10000
[W] Worker emprestado registrado: <uuid> | origem: 10.62.206.13:10000
[W] Farm → locais=3 emprestados=2 total=5
[W] Worker <uuid> (emprestado) concluiu QUERY | STATUS=OK
[SAT] Carga normalizada (load=2). Devolvendo 2 worker(s).
[SAT] → command_release → worker <uuid>
[SAT] → notify_worker_returned → Master_B worker=<uuid>
[W] Farm → locais=3 emprestados=0 total=3
```

---

## Arquitetura de Threads (SPEC §4)

```
Master (main thread)
├── start_worker_server()      → bloqueia o processo principal
│   └── handle_worker(conn)    → 1 thread por Worker (S1 + S2 + S3-T04)
├── start_master_server()      → thread daemon (Sprint 3 T01)
│   └── handle_master(conn)    → 1 thread por Master vizinho
├── simulate_load()            → thread daemon (Sprint 2 T02)
└── saturation_monitor()       → thread daemon (Sprint 3 T02)

Worker (objeto WorkerClient)
├── _connect()                 → loop de reconexão (Sprint 1 T04)
├── _reader_thread()           → roteia msgs para response_q ou command_q
├── _heartbeat_loop()          → envia HEARTBEAT a cada 30s (Sprint 1 T02)
└── _task_cycle()              → ALIVE → QUERY/NO_TASK → STATUS → ACK
```

## Fluxo de Mensagens por Sprint

### Sprint 1
```
Worker ──► Master : {"SERVER_UUID": "Master_A", "TASK": "HEARTBEAT"}\n
Master ──► Worker : {"SERVER_UUID": "Master_A", "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}\n
```

### Sprint 2
```
Worker ──► Master : {"WORKER": "ALIVE", "WORKER_UUID": "<uuid>"}\n
Master ──► Worker : {"TASK": "QUERY", "USER": "Alice"}\n
Worker ──► Master : {"STATUS": "OK", "TASK": "QUERY", "WORKER_UUID": "<uuid>"}\n
Master ──► Worker : {"STATUS": "ACK", "WORKER_UUID": "<uuid>"}\n
```

### Sprint 3
```
Master_A ──► Master_B : {"type": "request_help",   "request_id": "<uuid>", "payload": {...}}\n
Master_B ──► Master_A : {"type": "response_accepted","request_id": "<uuid>", "payload": {...}}\n
Master_B ──► Worker   : {"type": "command_redirect", "request_id": "<uuid>", "payload": {...}}\n
Worker   ──► Master_A : {"type": "register_temporary_worker", ...}\n
Master_A ──► Worker   : {"type": "command_release",  "request_id": "<uuid>", "payload": {...}}\n
Master_A ──► Master_B : {"type": "notify_worker_returned", "request_id": "<uuid>", "payload": {...}}\n
```
