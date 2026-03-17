import socket
import threading
import json
import time

HOST = "127.0.0.1"
PORT = 5000

workers = []
lock = threading.Lock()

def handle_worker(conn, addr):
    print(f"Worker conectado: {addr}")

    buffer = ""

    while True:
        try:
            data = conn.recv(1024).decode()
            if not data:
                break

            buffer += data

            while "\n" in buffer:
                msg, buffer = buffer.split("\n", 1)

                payload = json.loads(msg)

                if payload["TASK"] == "REGISTER":
                    with lock:
                        workers.append(conn)
                    print(f"Worker registrado {addr}")

                elif payload["TASK"] == "HEARTBEAT":

                    response = {
                        "SERVER_UUID": "MASTER_LOCAL",
                        "TASK": "HEARTBEAT",
                        "RESPONSE": "ALIVE"
                    }

                    conn.send((json.dumps(response) + "\n").encode())

                elif payload["TASK"] == "RESULT":
                    print(f"Resultado recebido do worker: {payload['RESULT']}")

        except:
            break

    conn.close()

    with lock:
        if conn in workers:
            workers.remove(conn)

    print(f"Worker desconectado: {addr}")


def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen()

    print(f"Master rodando em {HOST}:{PORT}")

    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_worker, args=(conn, addr))
        thread.start()


def simulate_load():

    while True:

        time.sleep(8)

        with lock:
            if len(workers) == 0:
                print("Nenhum worker disponível")
                continue

            worker = workers[0]

        task = {
            "TASK": "PROCESS",
            "DATA": 5
        }

        try:
            worker.send((json.dumps(task) + "\n").encode())
            print("Tarefa enviada ao worker")
        except:
            pass


if __name__ == "__main__":

    threading.Thread(target=simulate_load, daemon=True).start()
    start_server()