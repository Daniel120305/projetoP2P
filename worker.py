import socket
import json
import time
import random
import uuid
import threading
import queue
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# Configuração
# ─────────────────────────────────────────────────────────────────────────────
MASTER_HOST        = "10.62.206.13"
MASTER_PORT        = 10000
HEARTBEAT_INTERVAL = 30          # segundos entre heartbeats (Sprint 1)
WORKER_UUID        = str(uuid.uuid4())


# ─────────────────────────────────────────────────────────────────────────────
# Utilitário de log
# ─────────────────────────────────────────────────────────────────────────────
def log(msg: str) -> None:
    ts    = datetime.now().strftime("%H:%M:%S")
    short = WORKER_UUID[:8]
    print(f"[{ts}] [WORKER {short}] {msg}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# Classe WorkerClient
# ─────────────────────────────────────────────────────────────────────────────
class WorkerClient:
    """
    Encapsula toda a lógica do Worker:
    - Sprint 1: heartbeat periódico via TCP
    - Sprint 2: ciclo ALIVE → QUERY/NO_TASK → STATUS → ACK
    - Sprint 3: command_redirect / command_release / register_temporary_worker
    """

    def __init__(
        self,
        host:          str,
        port:          int,
        master_id:     str,
        is_borrowed:   bool = False,
        orig_master_id: str  = None,
        orig_host:     str  = None,
        orig_port:     int  = None,
    ):
        self.host          = host
        self.port          = port
        self.master_id     = master_id
        self.is_borrowed   = is_borrowed

        # Endereço e ID do Master original (para devolução — Sprint 3)
        self.orig_master_id = orig_master_id or master_id
        self.orig_host      = orig_host or host
        self.orig_port      = orig_port or port

        self.conn:      socket.socket | None = None
        self.send_lock: threading.Lock = threading.Lock()
        self.active:    bool           = False

        # Filas internas de mensagens (usadas pela thread leitora)
        self.response_q: queue.Queue = queue.Queue()   # respostas esperadas (HEARTBEAT, QUERY, ACK…)
        self.command_q:  queue.Queue = queue.Queue()   # comandos não solicitados (redirect, release)

    # ── Conexão ───────────────────────────────────────────────────────────────
    def _connect(self) -> None:
        """Conecta ao Master, tentando indefinidamente até conseguir."""
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

    # ── Envio seguro ──────────────────────────────────────────────────────────
    def _send(self, data: dict) -> bool:
        with self.send_lock:
            try:
                self.conn.send((json.dumps(data) + "\n").encode("utf-8"))
                return True
            except Exception as e:
                log(f"Erro ao enviar: {e}")
                self.active = False
                return False

    # ── Thread leitora ────────────────────────────────────────────────────────
    def _reader_thread(self) -> None:
        """
        Lê continuamente da socket e distribui mensagens:
        - command_redirect / command_release  → command_q (não solicitados)
        - HEARTBEAT ALIVE                     → tratado aqui (log imediato)
        - todo o resto                        → response_q (resposta esperada)
        """
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

                        # Comandos não solicitados (Sprint 3)
                        if mtype in ("command_redirect", "command_release"):
                            self.command_q.put(msg)

                        # Resposta de HEARTBEAT: tratar inline sem mexer na response_q
                        elif msg.get("TASK") == "HEARTBEAT" and msg.get("RESPONSE") == "ALIVE":
                            log("Status: ALIVE")

                        # Tudo o mais: colocar na fila para quem está esperando
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

        # Desbloqueia qualquer thread aguardando resposta
        self.response_q.put(None)

    # ── Sprint 1: Heartbeat periódico ─────────────────────────────────────────
    def _heartbeat_loop(self) -> None:
        """Envia HEARTBEAT a cada HEARTBEAT_INTERVAL segundos."""
        while self.active:
            time.sleep(HEARTBEAT_INTERVAL)
            if not self.active:
                break
            payload = {
                "SERVER_UUID": self.master_id,
                "TASK":        "HEARTBEAT",
            }
            if self._send(payload):
                log(f"Heartbeat enviado para {self.master_id}")
            else:
                log("Status: OFFLINE - Tentando Reconectar")
                self.active = False
                break

    # ── Sprint 3 T04: Registrar como worker temporário ────────────────────────
    def _register_temporary(self) -> None:
        msg = {
            "type":       "register_temporary_worker",
            "request_id": str(uuid.uuid4()),
            "payload": {
                "worker_id":               WORKER_UUID,
                "original_master_address": f"{self.orig_host}:{self.orig_port}",
            },
        }
        if self._send(msg):
            log(f"register_temporary_worker enviado | origem: {self.orig_host}:{self.orig_port}")

    # ── Sprint 2: Ciclo de tarefa ─────────────────────────────────────────────
    def _task_cycle(self):
        """
        Uma iteração do ciclo de tarefa:
        ALIVE → (QUERY | NO_TASK | command) → STATUS → ACK

        Retorna:
          None                  → ciclo normal, continuar
          ("redirect", addr)    → recebeu command_redirect
          ("release", addr)     → recebeu command_release
        """
        # Verificar comandos pendentes antes de solicitar tarefa
        try:
            cmd = self.command_q.get_nowait()
            return self._handle_command(cmd)
        except queue.Empty:
            pass

        # Sprint 2 T01: enviar apresentação ALIVE
        alive: dict = {"WORKER": "ALIVE", "WORKER_UUID": WORKER_UUID}
        if self.is_borrowed:
            alive["SERVER_UUID"] = self.orig_master_id    # identifica master de origem
        self._send(alive)
        log(f"ALIVE enviado "
            f"{'(emprestado de ' + self.orig_master_id + ')' if self.is_borrowed else '(local)'}")

        # Aguardar resposta do Master (timeout 5s — Sprint 2 nota de impl.)
        try:
            resp = self.response_q.get(timeout=5)
        except queue.Empty:
            log("Timeout aguardando resposta do Master")
            return None

        if resp is None:
            log("Conexão perdida enquanto aguardava resposta")
            self.active = False
            return None

        # Verificar se é um comando não solicitado chegando pela response_q
        mtype = resp.get("type", "")
        if mtype in ("command_redirect", "command_release"):
            return self._handle_command(resp)

        task = resp.get("TASK", "")

        # Sprint 2 T02: sem tarefa disponível
        if task == "NO_TASK":
            log("Sem tarefas disponíveis")
            return None

        # Sprint 2 T03: processar QUERY
        if task == "QUERY":
            user = resp.get("USER", "desconhecido")
            log(f"Processando QUERY para usuário: {user}")

            proc_time = random.uniform(1, 3)
            time.sleep(proc_time)

            # Resultado aleatório: 90 % OK, 10 % NOK
            status = "OK" if random.random() > 0.1 else "NOK"
            self._send({
                "STATUS":      status,
                "TASK":        "QUERY",
                "WORKER_UUID": WORKER_UUID,
            })
            log(f"STATUS {status} enviado")

            # Sprint 2 T04: aguardar ACK
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

    # ── Sprint 3: tratar comandos do Master ───────────────────────────────────
    def _handle_command(self, cmd: dict):
        mtype = cmd.get("type", "")
        p     = cmd.get("payload", {})

        if mtype == "command_redirect":
            addr = p.get("new_master_address", "")
            log(f"command_redirect recebido → novo Master: {addr}")
            return ("redirect", addr)

        if mtype == "command_release":
            addr = p.get("original_master_address", "")
            log(f"command_release recebido → retornando para: {addr}")
            return ("release", addr)

        return None

    # ── Ciclo principal ───────────────────────────────────────────────────────
    def run(self):
        """
        Conecta ao Master, inicia threads auxiliares e executa o ciclo de tarefas.
        Retorna quando ocorre redirect ou release (para que main() trate o caso).
        """
        self._connect()

        # Sprint 3: se emprestado, anunciar-se ao novo Master
        if self.is_borrowed:
            self._register_temporary()

        # Sprint 1: thread de heartbeat periódico
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        # Thread leitora (roteia mensagens para as filas corretas)
        threading.Thread(target=self._reader_thread, daemon=True).start()

        # Ciclo de tarefas (main loop)
        result = None
        while self.active:
            try:
                result = self._task_cycle()
                if isinstance(result, tuple):
                    break           # redirect ou release → main() decide
                time.sleep(5)       # aguardar antes de solicitar próxima tarefa
            except Exception as e:
                log(f"Erro no ciclo de tarefas: {e}")
                break

        # Desligar graciosamente
        self.active = False
        try:
            self.conn.close()
        except Exception:
            pass

        return result


# ─────────────────────────────────────────────────────────────────────────────
# Função principal — gerencia reconexões e transições de estado
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    log(f"Worker iniciado | UUID: {WORKER_UUID}")

    # Estado atual da conexão
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

            if action == "redirect":
                # Sprint 3 T04: conectar ao novo Master como worker emprestado
                log(f"Redirecionando para: {addr}")
                if ":" in addr:
                    h, p = addr.rsplit(":", 1)
                    host, port = h, int(p)
                else:
                    log(f"Endereço de redirecionamento inválido: '{addr}'. Usando padrão.")
                master_id   = addr  # ID real desconhecido antes do primeiro HEARTBEAT
                is_borrowed = True
                # orig_id, orig_host, orig_port permanecem inalterados

            elif action == "release":
                # Sprint 3 T05: retornar ao Master original
                log(f"Liberado. Retornando ao Master original: {orig_host}:{orig_port}")
                host        = orig_host
                port        = orig_port
                master_id   = orig_id
                is_borrowed = False

        else:
            # Desconexão inesperada — aguardar antes de tentar reconectar
            log("Conexão encerrada. Reconectando em 5s…")
            time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
