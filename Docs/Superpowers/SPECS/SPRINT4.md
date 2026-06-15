# Sprint 4 — Reporter de Métricas para o Supervisor do Cluster

**Disciplina:** Arquitetura de Sistemas Distribuídos · **Prof.** Michel Junio Ferreira Rosa
**Projeto:** `projetoP2P` · **Entrega/Apresentação:** 15/06/2026 (turmas B e UN)

---

## 1. Objetivo

Cada Master passa a enviar, **a cada 10 segundos**, um relatório `performance_report`
em JSON para o Supervisor de Métricas do professor, via **TLS sobre TCP**. O nó NÃO
aguarda resposta: conecta, envia, fecha. As métricas aparecem no dashboard web do
professor automaticamente.

Não há UI a construir nesta sprint — o dashboard é do professor. O entregável é o
**cliente reporter** + a **instrumentação de contadores** no `master.py`.

### Parâmetros de conexão (fixos pela especificação)

| Parâmetro | Valor |
|---|---|
| Host | `nuted-ia.dev` |
| Porta | `443` |
| Protocolo | TLS sobre TCP |
| SNI | `nuted-ia.dev` |
| Resposta | **não** fazer `recv` — apenas `send` e `close` |
| HTTP | **proibido** — é socket TLS puro, sem caminho de URL |

---

## 2. Restrições importantes (NÃO violar)

1. **Sem HTTP / sem bibliotecas HTTP.** Usar `socket` + `ssl` da stdlib. Nada de
   `requests`, `urllib`, `http.client`.
2. **Sem caminho/endpoint.** Em TCP o destino é só `host:porta`. Não usar
   `/supervisor/...`.
3. **Fire-and-forget.** Após `sendall`, **não** chamar `recv`. Fechar a conexão.
4. O envio ao supervisor **roda em thread própria** e **nunca** pode derrubar o
   Master se a rede falhar (envolver tudo em `try/except` + log).
5. Reaproveitar o estilo do repo: cabeçalhos em caixa, comentários por Sprint/Tarefa,
   logs via a função `log()` já existente, locks para acesso a estado compartilhado.

---

## 3. Arquivos a criar / alterar

- `requirements.txt` (criar) — `psutil>=5.9`.
- `supervisor.py` (criar) — isolamento do TLS e das métricas de sistema (psutil).
  Recebe o estado da farm por **callback** (`get_state`) para evitar import circular.
- `master.py` (alterar) — instrumentação de contadores + `collect_state()` + ligação
  do reporter no `__main__`.

Mapeamento detalhado de funções, contadores e pontos de instrumentação: ver §4 e o
código implementado (`supervisor.py` e blocos `# Sprint 4 — ...` em `master.py`).

---

## 4. Mapeamento campo → fonte (referência rápida)

| Campo do payload | Fonte no código |
|---|---|
| `uptime_seconds` | `time.time() - START_TIME` |
| `cpu.*`, `memory.*`, `disk.*`, `load_average_*` | `psutil` em `supervisor.build_system_metrics` |
| `workers.total_registered` / `workers_alive` | `len(workers)` |
| `workers_received` | workers com `is_borrowed=True` |
| `workers_borrowed` (out) | `len(lent_out)` |
| `workers_home` | locais presentes + emprestados p/ fora |
| `workers_utilization` / `tasks_running` | contador `tasks_running` |
| `workers_idle` / `available_capacity` | `alive - running` |
| `tasks_pending` | `task_queue.qsize()` |
| `tasks_completed` / `tasks_failed` | contadores em `STATUS OK/NOK` |
| `oldest_task_age_s` | `task_timestamps[0]` |
| `config_thresholds.max_task` | `CAPACITY` |
| `config_thresholds.release_task` | `RELEASE_THRESHOLD` |
| `neighbors[]` | `NEIGHBOR_MASTERS` + `m2m_conns` |

---

## 5. Definição de Pronto (DoD)

1. `pip install -r requirements.txt` instala `psutil`.
2. Subir um Master: a cada 10s aparece no log algo como `Reporter ... ok` e **nenhum**
   traceback de rede derruba o processo.
3. Abrir `https://nuted-ia.dev/supervisor/dashboard/` e ver o nó (`server_uuid`) UP,
   com CPU/memória/disco e contadores de workers/tarefas atualizando.
4. Gerar carga (deixar `simulate_load` rodando): `tasks_pending`, `tasks_running` e
   `tasks_completed` mudam no dashboard.
5. Com 2 Masters + empréstimo de worker, o nó que recebeu mostra `workers_received≥1`
   e `borrowed_workers` com `"direction":"in"`; o que emprestou mostra
   `workers_borrowed≥1` e `"direction":"out"`.
6. Derrubar a rede do supervisor (ou tirar o Wi-Fi) NÃO trava o Master — só loga falha.

---

## 6. Decisões (resolvidas pelo PDF oficial `plano_proj_SD-26_1.pdf`)

1. **`server_uuid` → RESOLVIDO:** o PDF (p.23) diz que `michel_1`/`michel_2`
   **devem** ser usados no `server_uuid`. Código ajustado: `SERVER_UUID = "michel_1"`
   (e `hostname = "michel_1.farm.local"`). Use `michel_2` no segundo master da dupla.
2. **Porta → 443 (confirmado funcionando):** a p.17 menciona "porta 8000", mas o bloco
   oficial de "Parâmetros da conexão" (p.22) define **Host `nuted-ia.dev`, Porta `443`,
   TLS, SNI `nuted-ia.dev`**. O envio real ao 443 retornou `ok`, confirmando que 443/TLS
   é o correto. O "8000" é resíduo de versão anterior.
3. **`payload_version` → ambíguo no PDF:** o exemplo de payload (p.17) usa
   `"sprint4-monitor"`, mas a tabela de campos (p.19) cita `"sprint4-monitor-v2"`.
   Mantido `"sprint4-monitor"` (igual ao exemplo concreto). Trocar em 1 linha em
   `supervisor.py` se o dashboard exigir `-v2`.
4. **`config_thresholds` (max_task/release_task):** mapeados para os thresholds DESTE
   projeto (`CAPACITY=10`, `RELEASE_THRESHOLD=4`) — os valores `100`/`60` do exemplo são
   da farm do professor. Semântica idêntica à descrição da p.22.
5. **Delimitador `\n` ao supervisor:** mantido por consistência. Se o dashboard
   reclamar, remover o `+ "\n"` em `supervisor.send_report`.
