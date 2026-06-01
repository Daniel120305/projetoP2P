# Plano de Implementação — P2P Balanceamento de Carga

**Projeto:** projetoP2P  
**Baseado em:** Sprint 01, 02 e 03 do PDF do professor  

---

## Estado Inicial (código no GitHub)

| Arquivo | O que existe | O que está faltando/errado |
|---|---|---|
| `master.py` | TCP server, threads, `\n` delimiter, HEARTBEAT ✓ | Payload HEARTBEAT incompleto; usa `PROCESS`/`RESULT` (fora do protocolo); sem fila; sem Sprint 2; sem Sprint 3 |
| `worker.py` | TCP client, `\n` parsing | Usa `REGISTER` (fora do protocolo); sem WORKER_UUID; sem loop; sem reconexão; sem heartbeat periódico; sem Sprint 2; sem Sprint 3 |

---

## Sprint 1 — Heartbeat (Infraestrutura TCP + Heartbeat)

### Tarefa 01 — Infraestrutura TCP ✅ (existente, ajuste de payload)
- [x] Master escuta em porta definida (5000)
- [x] Worker conecta como cliente
- [x] Delimitador `\n` implementado nos dois lados
- **Ajuste:** corrigir payload HEARTBEAT para incluir `SERVER_UUID`

### Tarefa 02 — Lógica de Requisição (Worker → Master)
- [ ] Worker envia `{"SERVER_UUID": "<master_id>", "TASK": "HEARTBEAT"}`
- [ ] Heartbeat periódico a cada 30s em thread separada
- [ ] Log: `"Heartbeat enviado para <master_id>"`

### Tarefa 03 — Lógica de Resposta (Master → Worker)
- [x] Master identifica `TASK == "HEARTBEAT"` (existe)
- [ ] Resposta: `{"SERVER_UUID": "<master_id>", "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}`
- [ ] Log do worker: `"Status: ALIVE"` ou `"Status: OFFLINE - Tentando Reconectar"`

### Tarefa 04 — Concorrência e Resiliência
- [x] Master usa threads por worker (existe)
- [ ] Worker usa thread separada para heartbeat (não bloqueia ciclo de tarefa)
- [ ] Worker reconecta automaticamente com loop + sleep(5)

---

## Sprint 2 — Ciclo de Tarefas

### Tarefa 01 — Apresentação e Identificação (Worker → Master)
- [ ] Worker gera WORKER_UUID único (`uuid.uuid4()`)
- [ ] Envia `{"WORKER": "ALIVE", "WORKER_UUID": "..."}`
- [ ] Se emprestado, inclui `"SERVER_UUID": "<master_origem>"`
- [ ] Master registra worker no dicionário `workers`

### Tarefa 02 — Distribuição de Carga (Master → Worker)
- [ ] `task_queue = queue.Queue()` para tarefas pendentes
- [ ] `simulate_load()` popula a fila continuamente
- [ ] Master responde `{"TASK": "QUERY", "USER": "..."}` se há tarefa
- [ ] Master responde `{"TASK": "NO_TASK"}` se fila vazia

### Tarefa 03 — Simulação de Processamento (Worker → Master)
- [ ] Worker simula processamento com `time.sleep(random.uniform(1, 3))`
- [ ] Envia `{"STATUS": "OK|NOK", "TASK": "QUERY", "WORKER_UUID": "..."}`
- [ ] Log: tarefa concluída com status

### Tarefa 04 — ACK e Persistência (Master → Worker)
- [ ] Master envia `{"STATUS": "ACK", "WORKER_UUID": "..."}`
- [ ] Master loga: worker local ou emprestado que concluiu a tarefa
- [ ] Worker recebe ACK e fecha o ciclo

---

## Sprint 3 — Protocolo Master-to-Master

### Tarefa 01 — Conexão TCP entre Masters
- [ ] Master escuta segunda porta (5001) para conexões M2M
- [ ] Diretório de vizinhos: `NEIGHBOR_MASTERS = {"Master_B": {"host": ..., "master_port": ..., "worker_port": ...}}`
- [ ] Reusar delimitador `\n` para mensagens M2M
- [ ] `handle_master(conn, addr)` em thread separada

### Tarefa 02 — Detecção de Saturação
- [ ] `SATURATION_THRESHOLD = 10`, `RELEASE_THRESHOLD = 4` (histerese)
- [ ] `saturation_monitor()` roda a cada 5s em thread daemon
- [ ] Ao detectar `load > SATURATION_THRESHOLD`: calcular `workers_needed` e disparar `request_help`
- [ ] Ao detectar `load < RELEASE_THRESHOLD` com workers emprestados: iniciar devolução

### Tarefa 03 — Protocolo de Negociação
- [ ] Emitir `request_help` com UUID v4 como `request_id`
- [ ] `pending_m2m[request_id]` com `threading.Event` para await da resposta
- [ ] Timeout de 5s: se sem resposta, logar e tentar próximo vizinho
- [ ] Master recebendo `request_help`: avaliar workers ociosos, responder `response_accepted` ou `response_rejected`
- [ ] `response_accepted` mantém mesmo `request_id`

### Tarefa 04 — Redirecionamento de Workers
- [ ] Master B envia `command_redirect` a cada worker selecionado
- [ ] Worker trata `command_redirect` via `command_q`: desconecta graciosamente
- [ ] Worker conecta ao novo Master e envia `register_temporary_worker`
- [ ] Master A registra worker como emprestado + `original_master`
- [ ] Worker passa a enviar ALIVE com `SERVER_UUID = master_origem`

### Tarefa 05 — Devolução do Worker
- [ ] `saturation_monitor` detecta `load < RELEASE_THRESHOLD`
- [ ] Master A envia `command_release` ao worker emprestado
- [ ] Master A envia `notify_worker_returned` ao Master B (via conexão M2M mantida)
- [ ] Worker reconecta ao Master original sem perda de estado

### Tarefa 06 — Concorrência e Resiliência
- [ ] `send_lock` por conexão: evita interleaving de writes concorrentes
- [ ] `state_lock` protege dicionários compartilhados
- [ ] Desconexão inesperada de worker emprestado → tentar reconectar ao Master B
- [ ] Mensagem com `type` desconhecido → log + ignore (não derruba processo)

### Tarefa 07 — Logs e Observabilidade
- [ ] Todo envio/recebimento M2M loga `type`, `request_id[:8]`, timestamp
- [ ] Contador de workers locais vs emprestados exibido a cada mudança
- [ ] Ciclo de vida completo de worker emprestado: empréstimo → tarefas → devolução

---

## Critérios de Conclusão (DoD Consolidado)

### Sprint 1
- [x] Worker abre conexão TCP com Master
- [x] Master recebe JSON, faz parsing e identifica HEARTBEAT
- [x] Worker recebe ALIVE e imprime no log
- [x] Conexão mantida/reestabelecida sem travar processos

### Sprint 2
- [ ] Worker realiza handshake de apresentação com WORKER_UUID
- [ ] Master distribui tarefa da fila ou informa NO_TASK corretamente
- [ ] Worker processa e Master recebe STATUS OK/NOK
- [ ] Worker recebe ACK final sem erros de parsing
- [ ] Sistema trata SERVER_UUID (presente ou ausente)

### Sprint 3
- [ ] Master saturado envia request_help corretamente formatado
- [ ] Master vizinho responde response_accepted/rejected com mesmo request_id
- [ ] Workers redirecionados enviam register_temporary_worker e recebem tarefas
- [ ] Devolução: command_release + notify_worker_returned funciona
- [ ] Parsing tolera campos desconhecidos, falha controlada se campos obrigatórios ausentes
- [ ] Sem vazamento de threads, conexões pendentes ou perda de mensagens

---

## Arquivos Produzidos

| Arquivo | Descrição |
|---|---|
| `master.py` | Master reescrito com Sprint 1+2+3 |
| `worker.py` | Worker reescrito com Sprint 1+2+3 |
| `SPEC.md` | Especificação técnica completa |
| `PLAN.md` | Este arquivo — plano de implementação |

---

## Como Executar

```bash
# Terminal 1 — Master A (porta worker 5000, porta M2M 5001)
python master.py

# Terminal 2 — Master B (porta worker 5010, porta M2M 5011)
# Editar WORKER_PORT=5010, MASTER_PORT=5011, MASTER_ID="Master_B" em master.py
python master.py

# Terminal 3+ — Workers
python worker.py
```

Para testar Sprint 3, configurar `NEIGHBOR_MASTERS` em cada master com o endereço do vizinho.
