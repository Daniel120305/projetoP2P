import socket
import json
import time
import random

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000


def connect_master():

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((MASTER_HOST, MASTER_PORT))

    register = {
        "TASK": "REGISTER"
    }

    client.send((json.dumps(register) + "\n").encode())

    return client


def worker_loop(client):

    buffer = ""

    

    data = client.recv(1024).decode()

    buffer += data

    while "\n" in buffer:

        msg, buffer = buffer.split("\n", 1)

        payload = json.loads(msg)

        if payload["TASK"] == "HEARTBEAT":

            response = {
                "TASK": "HEARTBEAT",
                "RESPONSE": "ALIVE"
            }

            client.send((json.dumps(response) + "\n").encode())

        elif payload["TASK"] == "PROCESS":

            n = payload["DATA"]

            print(f"Processando tarefa: {n}")

            time.sleep(random.randint(2,5))

            result = n * n

            response = {
                "TASK": "RESULT",
                "RESULT": result
            }

            client.send((json.dumps(response) + "\n").encode())


if __name__ == "__main__":

    client = connect_master()

    print("Worker conectado ao master")

    worker_loop(client)