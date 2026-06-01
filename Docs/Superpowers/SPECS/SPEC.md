# Especificação Técnica — Sistema P2P com Balanceamento de Carga Dinâmico

**Disciplina:** Arquitetura de Sistemas Distribuídos  
**Professor:** Michel Junio Ferreira Rosa  
**Versão:** 1.0  

---

## 1. Visão Geral

Sistema distribuído em Python com dois tipos de nó: **Master** (servidor/orquestrador) e **Worker** (cliente/executor). Cada Master gerencia uma Farm de Workers e, ao atingir saturação, negocia via protocolo TCP o empréstimo de Workers de Masters vizinhos.

---

## 2. Entidades e Responsabilidades

### 2.1 Nó Master

| Responsabilidade | Sprint |
|---|---|
| Servidor TCP para Workers (porta 5000) | S1 |
| Servidor TCP para Masters vizinhos (porta 5001) | S3 |
| Responder HEARTBEAT com ALIVE | S1 |
| Manter fila de tarefas (`queue.Queue`) | S2 |
| Entregar QUERY ou NO_TASK ao Worker | S2 |
| Receber STATUS e enviar ACK | S2 |
| Detectar saturação (load > threshold) | S3 |
| Enviar `request_help` a Master vizinho | S3 |
| Responder `response_accepted` ou `response_rejected` | S3 |
| Enviar `command_redirect` a Worker ofertado | S3 |
| Registrar Worker temporário (`register_temporary_worker`) | S3 |
| Enviar `command_release` quando carga normaliza | S3 |
| Enviar `notify_worker_returned` ao Master de origem | S3 |

### 2.2 Nó Worker

| Responsabilidade | Sprint |
|---|---|
| Conectar ao Master e reconectar em falha | S1 |
| Enviar HEARTBEAT periódico (a cada 30s) | S1 |
| Enviar ALIVE com WORKER_UUID | S2 |
| Incluir SERVER_UUID se emprestado | S2 |
| Processar QUERY (sleep aleatório) | S2 |
| Reportar STATUS (OK/NOK) e aguardar ACK | S2 |
| Tratar `command_redirect` → reconectar ao novo Master | S3 |
| Enviar `register_temporary_worker` ao novo Master | S3 |
| Tratar `command_release` → retornar ao Master original | S3 |

---

## 3. Protocolo de Mensagens

### Sprint 1 — Heartbeat (Worker ↔ Master)

```
Worker → Master:  {"SERVER_UUID": "<master_id>", "TASK": "HEARTBEAT"}\n
Master → Worker:  {"SERVER_UUID": "<master_id>", "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}\n
```

### Sprint 2 — Ciclo de Tarefas (Worker ↔ Master)

```
Worker → Master:  {"WORKER": "ALIVE", "WORKER_UUID": "<uuid>"}              # local
Worker → Master:  {"WORKER": "ALIVE", "WORKER_UUID": "<uuid>", "SERVER_UUID": "<origin_master_id>"}  # emprestado

Master → Worker:  {"TASK": "QUERY", "USER": "<nome>"}   # com tarefa
Master → Worker:  {"TASK": "NO_TASK"}                   # fila vazia

Worker → Master:  {"STATUS": "OK|NOK", "TASK": "QUERY", "WORKER_UUID": "<uuid>"}
Master → Worker:  {"STATUS": "ACK", "WORKER_UUID": "<uuid>"}
```

### Sprint 3 — Negociação Master-to-Master

```json
// Pedido de ajuda
{"type": "request_help", "request_id": "<uuid4>",
 "payload": {"master_id": "A", "current_load": 150, "capacity": 100,
             "workers_needed": 2, "worker_address": "ip:porta_worker"}}

// Resposta aceita
{"type": "response_accepted", "request_id": "<mesmo_uuid>",
 "payload": {"workers_offered": 2, "worker_details": [{"id":"W1","address":"ip:port"}]}}

// Resposta rejeitada
{"type": "response_rejected", "request_id": "<mesmo_uuid>",
 "payload": {"reason": "high_load|no_workers_available|refused"}}

// Redirecionar Worker
{"type": "command_redirect", "request_id": "<novo_uuid>",
 "payload": {"new_master_address": "ip:porta_worker_masterA"}}

// Worker registra-se no novo Master
{"type": "register_temporary_worker", "request_id": "<novo_uuid>",
 "payload": {"worker_id": "<uuid>", "original_master_address": "ip:porta_original"}}

// Liberar Worker
{"type": "command_release", "request_id": "<novo_uuid>",
 "payload": {"original_master_address": "ip:porta_masterB"}}

// Notificar Master de origem
{"type": "notify_worker_returned", "request_id": "<novo_uuid>",
 "payload": {"worker_id": "<uuid>"}}
```

---

## 4. Arquitetura de Concorrência

```
Master (main thread)
├── start_worker_server()        → aceita conns de Workers
│   └── handle_worker(conn)      → 1 thread por Worker [Sprint 1 + S2 + S3]
├── start_master_server()        → aceita conns de Masters (thread)
│   └── handle_master(conn)      → 1 thread por Master vizinho [Sprint 3]
├── simulate_load()              → adiciona tarefas à fila (thread daemon)
└── saturation_monitor()         → detecta saturação / liberação (thread daemon)

Worker (objeto WorkerClient)
├── connect()                    → loop de reconexão
├── reader_thread()              → lê da socket; roteia msgs para response_q ou command_q
├── heartbeat_loop()             → envia HEARTBEAT a cada 30s (thread daemon)
└── task_cycle()                 → ALIVE → QUERY/NO_TASK → STATUS → ACK (loop principal)
```

---

## 5. Thresholds e Histerese

| Parâmetro | Valor padrão | Descrição |
|---|---|---|
| `CAPACITY` | 10 | Capacidade nominal da fila |
| `SATURATION_THRESHOLD` | 10 | load > este valor → solicitar ajuda |
| `RELEASE_THRESHOLD` | 4 | load < este valor → devolver Workers |

A diferença entre os thresholds (10 vs 4) implementa **histerese**: evita que o mesmo Worker seja emprestado e devolvido em ciclos rápidos (efeito ping-pong).

---

## 6. Regras de Parsing

- Todo JSON termina com `\n` (delimitador de stream TCP)
- Campos desconhecidos: **ignorados** (compatibilidade futura)
- Campos obrigatórios ausentes: log de erro, mensagem descartada, processo não derruba
- Case sensitivity: valores de controle em MAIÚSCULAS (ALIVE, QUERY, NO_TASK, OK, NOK, ACK); tipos M2M em minúsculas (request_help, response_accepted, etc.)
- Timeout de resposta: **5 segundos** para todas as esperas

---

## 7. Casos de Teste Cobertos

| ID | Cenário |
|---|---|
| CT-S1-01 | Worker abre conexão TCP, envia HEARTBEAT, recebe ALIVE |
| CT-S1-02 | Master offline → Worker loga OFFLINE e tenta reconectar |
| CT-S2-01 | Worker ALIVE local → Master entrega QUERY → Worker OK → ACK |
| CT-S2-02 | Worker ALIVE → fila vazia → NO_TASK |
| CT-S2-03 | Worker ALIVE emprestado (SERVER_UUID) → Master entrega QUERY |
| CT-S3-01 | Master saturado envia request_help, vizinho aceita |
| CT-S3-02 | Master saturado envia request_help, vizinho rejeita (high_load) |
| CT-S3-03 | Worker recebe command_redirect, conecta ao novo Master |
| CT-S3-04 | Worker envia register_temporary_worker ao novo Master |
| CT-S3-05 | Carga normaliza → command_release + notify_worker_returned |
| CT-S3-06 | Timeout 5s em request_help → log e desiste |
| CT-S3-07 | Mensagem com type desconhecido → log + ignorar |
