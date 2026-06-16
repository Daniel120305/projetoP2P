import sys
import socket
import json
import time
import random
import uuid
import threading
import queue
from datetime import datetime

# Garante UTF-8 na saída: evita UnicodeEncodeError com acentos/símbolos quando
# o stdout não é UTF-8 (console cp1252 ou saída redirecionada para arquivo/pipe).
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 1 — Tarefa 01: Configuração da Conexão TCP
# Sprint 1 — Tarefa 02: Intervalo do Heartbeat
# Sprint 2 — Tarefa 01: WORKER_UUID único
# ═══════════════════════════════════════════════════════════════════════════════
# 127.0.0.1 = Master na MESMA máquina (teste local). Para um Master em outra máquina
# da rede, troque pelo IP dele (ex.: "10.62.206.213"). NÃO use "0.0.0.0" aqui.
MASTER_HOST        = "127.0.0.1"
MASTER_PORT        = 10000
HEARTBEAT_INTERVAL = 30                 # Sprint 1 T02 — heartbeat a cada 30s
WORKER_UUID        = str(uuid.uuid4())  # Sprint 2 T01 — UUID único por instância


def log(msg: str) -> None:
    ts    = datetime.now().strftime("%H:%M:%S")
    short = WORKER_UUID[:8]
    print(f"[{ts}] [WORKER {short}] {msg}", flush=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 1 — Tarefa 04: Concorrência e Resiliência (WorkerClient)
# Sprint 2 — Tarefa 01 a 04: Ciclo de Tarefas completo
# Sprint 3 — Tarefa 04: Redirecionamento (command_redirect / register_temporary)
# Sprint 3 — Tarefa 05: Devolução (command_release)
# Sprint 3 — Tarefa 06: send_lock por conexão, filas response_q / command_q
# ═══════════════════════════════════════════════════════════════════════════════
class WorkerClient:

    def __init__(
        self,
        host:           str,
        port:           int,
        master_id:      str,
        is_borrowed:    bool = False,
        orig_master_id: str  = None,
        orig_host:      str  = None,
        orig_port:      int  = None,
    ):
        self.host          = host
        self.port          = port
        self.master_id     = master_id
        self.is_borrowed   = is_borrowed

        # Sprint 3 T05 — endereço do Master original para devolução
        self.orig_master_id = orig_master_id or master_id
        self.orig_host      = orig_host or host
        self.orig_port      = orig_port or port

        self.conn:      socket.socket | None = None
        self.send_lock: threading.Lock = threading.Lock()  # Sprint 3 T06
        self.active:    bool           = False

        # Sprint 3 T06 — filas internas: separam respostas de comandos não solicitados
        self.response_q: queue.Queue = queue.Queue()  # QUERY, NO_TASK, ACK…
        self.command_q:  queue.Queue = queue.Queue()  # command_redirect, command_release

    # ── Sprint 1 T04: Reconexão automática com loop + sleep(5) ───────────────
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

    # ── Sprint 3 T06: Envio thread-safe (send_lock por conexão) ──────────────
    def _send(self, data: dict) -> bool:
        with self.send_lock:
            try:
                self.conn.send((json.dumps(data) + "\n").encode("utf-8"))
                return True
            except Exception as e:
                log(f"Erro ao enviar: {e}")
                self.active = False
                return False

    # ── Sprint 1 T04: Thread leitora — roteia mensagens para as filas certas ──
    def _reader_thread(self) -> None:
        buffer = ""
        while self.active:
            try:
                self.conn.settimeout(1.0)
                data = self.conn.recv(4096).decode("utf-8")
                if not data:
                    log("Conexão encerrada pelo Master")
                    self.active = False
                    break
                buffer += data
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg   = json.loads(line)
                        mtype = msg.get("type", "")

                        # Sprint 3 T04/05 — comandos não solicitados → command_q
                        if mtype in ("command_redirect", "command_release"):
                            self.command_q.put(msg)

                        # Sprint 1 T03 — resposta HEARTBEAT: tratar inline
                        elif msg.get("TASK") == "HEARTBEAT" and msg.get("RESPONSE") == "ALIVE":
                            log("Status: ALIVE")

                        # Sprint 2 — respostas esperadas pelo ciclo de tarefa
                        else:
                            self.response_q.put(msg)

                    except json.JSONDecodeError as e:
                        log(f"JSON inválido: {e}")

            except socket.timeout:
                continue
            except Exception as e:
                log(f"Erro no reader: {e}")
                self.active = False
                break

        self.response_q.put(None)  # desbloqueia threads aguardando resposta

    # ── Sprint 1 T02: Heartbeat periódico em thread separada (30s) ───────────
    def _heartbeat_loop(self) -> None:
        while self.active:
            time.sleep(HEARTBEAT_INTERVAL)
            if not self.active:
                break
            # Sprint 1 T02 — payload exato: SERVER_UUID + TASK
            if self._send({"SERVER_UUID": self.master_id, "TASK": "HEARTBEAT"}):
                log(f"Heartbeat enviado para {self.master_id}")
            else:
                # Sprint 1 T03 — log de falha
                log("Status: OFFLINE - Tentando Reconectar")
                self.active = False
                break

    # ── Sprint 3 T04: Registrar como Worker temporário no novo Master ─────────
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

    # ── Sprint 2 T01–04: Uma iteração do ciclo de tarefa ─────────────────────
    def _task_cycle(self):
        """
        ALIVE → (QUERY | NO_TASK | command) → STATUS → ACK
        Retorna None (ciclo normal), ("redirect", addr) ou ("release", addr).
        """
        # Verificar comandos pendentes antes de pedir tarefa
        try:
            return self._handle_command(self.command_q.get_nowait())
        except queue.Empty:
            pass

        # Sprint 2 T01 — enviar ALIVE com WORKER_UUID (+ SERVER_UUID se emprestado)
        alive: dict = {"WORKER": "ALIVE", "WORKER_UUID": WORKER_UUID}
        if self.is_borrowed:
            alive["SERVER_UUID"] = self.orig_master_id
        self._send(alive)
        log(f"ALIVE enviado "
            f"{'(emprestado de ' + self.orig_master_id + ')' if self.is_borrowed else '(local)'}")

        # Aguardar resposta do Master — timeout 5s (SPEC §6)
        try:
            resp = self.response_q.get(timeout=5)
        except queue.Empty:
            log("Timeout aguardando resposta do Master")
            return None

        if resp is None:
            log("Conexão perdida enquanto aguardava resposta")
            self.active = False
            return None

        # Comando não solicitado chegou pela response_q
        mtype = resp.get("type", "")
        if mtype in ("command_redirect", "command_release"):
            return self._handle_command(resp)

        task = resp.get("TASK", "")

        # Sprint 2 T02 — sem tarefa disponível
        if task == "NO_TASK":
            log("Sem tarefas disponíveis")
            return None

        # Sprint 2 T03 — processar QUERY com sleep aleatório
        if task == "QUERY":
            user = resp.get("USER", "desconhecido")
            log(f"Processando QUERY para usuário: {user}")
            time.sleep(random.uniform(1, 3))

            # Sprint 2 T03 — reportar STATUS OK (90 %) ou NOK (10 %)
            status = "OK" if random.random() > 0.1 else "NOK"
            self._send({"STATUS": status, "TASK": "QUERY", "WORKER_UUID": WORKER_UUID})
            log(f"STATUS {status} enviado")

            # Sprint 2 T04 — aguardar ACK (timeout 5s)
            try:
                ack = self.response_q.get(timeout=5)
                if ack and ack.get("STATUS") == "ACK":
                    log(f"ACK recebido ✓ | ciclo de tarefa concluído (status={status})")
                else:
                    log(f"ACK inesperado ou timeout: {ack}")
            except queue.Empty:
                log("Timeout aguardando ACK")
            return None

        log(f"Resposta inesperada: {resp}")
        return None

    # ── Sprint 3 T04/05: Tratar command_redirect e command_release ───────────
    def _handle_command(self, cmd: dict):
        mtype = cmd.get("type", "")
        p     = cmd.get("payload", {})

        # Sprint 3 T04 — redirecionar para novo Master
        if mtype == "command_redirect":
            addr = p.get("new_master_address", "")
            log(f"command_redirect recebido → novo Master: {addr}")
            return ("redirect", addr)

        # Sprint 3 T05 — retornar ao Master original
        if mtype == "command_release":
            addr = p.get("original_master_address", "")
            log(f"command_release recebido → retornando para: {addr}")
            return ("release", addr)

        return None

    # ── Sprint 1 T04: Inicia threads e executa ciclo principal ───────────────
    def run(self):
        self._connect()

        # Sprint 3 T04 — se emprestado, anunciar-se ao novo Master
        if self.is_borrowed:
            self._register_temporary()

        # Sprint 1 T02 — heartbeat periódico em thread daemon
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        # Sprint 1 T04 — thread leitora em thread daemon
        threading.Thread(target=self._reader_thread, daemon=True).start()

        result = None
        while self.active:
            try:
                result = self._task_cycle()
                if isinstance(result, tuple):
                    break           # redirect ou release — main() decide
                time.sleep(5)
            except Exception as e:
                log(f"Erro no ciclo de tarefas: {e}")
                break

        self.active = False
        try:
            self.conn.close()
        except Exception:
            pass

        return result


# ═══════════════════════════════════════════════════════════════════════════════
# Sprint 1 — Tarefa 04: Loop principal de reconexão
# Sprint 3 — Tarefa 04: Transição redirect → novo Master (worker emprestado)
# Sprint 3 — Tarefa 05: Transição release → Master original (devolução)
# ═══════════════════════════════════════════════════════════════════════════════
def main() -> None:
    log(f"Worker iniciado | UUID: {WORKER_UUID}")

    # Estado inicial de conexão
    host        = MASTER_HOST
    port        = MASTER_PORT
    master_id   = "Master_A"
    is_borrowed = False
    orig_id     = "Master_A"
    orig_host   = MASTER_HOST
    orig_port   = MASTER_PORT

    while True:
        client = WorkerClient(
            host           = host,
            port           = port,
            master_id      = master_id,
            is_borrowed    = is_borrowed,
            orig_master_id = orig_id,
            orig_host      = orig_host,
            orig_port      = orig_port,
        )
        result = client.run()

        if isinstance(result, tuple):
            action, addr = result

            # Sprint 3 T04 — conectar ao novo Master como worker emprestado
            if action == "redirect":
                log(f"Redirecionando para: {addr}")
                if ":" in addr:
                    h, p = addr.rsplit(":", 1)
                    host, port = h, int(p)
                else:
                    log(f"Endereço de redirecionamento inválido: '{addr}'. Usando padrão.")
                master_id   = addr          # addr como ID até o primeiro HEARTBEAT
                is_borrowed = True
                # orig_id, orig_host, orig_port permanecem inalterados

            # Sprint 3 T05 — retornar ao Master original
            elif action == "release":
                log(f"Liberado. Retornando ao Master original: {orig_host}:{orig_port}")
                host        = orig_host
                port        = orig_port
                master_id   = orig_id
                is_borrowed = False

        else:
            # Sprint 1 T04 — desconexão inesperada: aguardar e reconectar
            log("Conexão encerrada. Reconectando em 5s…")
            time.sleep(5)


# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    main()
