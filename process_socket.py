import socket
import threading
import json
import time
import random

class LamportProcessTCP(threading.Thread):
    def __init__(self, process_id, num_process, host="localhost", port_base=5000, log_file_path=None):
        super().__init__()
        self.process_id = process_id
        self.num_processes = num_process
        self.timestamp = 0
        self.queue = []  # List of dictionaries: {"timestamp": int, "process_id": int}
        self.acks_received = [0 for _ in range(self.num_processes)]
        self.host = host
        self.port_base = port_base
        self.port = port_base + process_id


        self.log_file_path = log_file_path
        if self.log_file_path:
            with open(self.log_file_path, "w") as f:
                f.write(f"Processo {self.process_id}\n")
        self.stop_ = False
        self.num_operations = 5
        self.n = 0  # Number of times it acquired the resource

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.num_processes**2)
        self.server_thread = threading.Thread(target=self.accept_request)
        self.server_thread.start()

        self.client_sockets = []

        
        threading.Thread(target=self.check_queue).start()

    def run(self):
        for i in range(self.num_processes):
            if i != self.process_id:
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect((self.host, self.port_base + i))
                self.client_sockets.append(client_socket)
            else:
                self.client_sockets.append(None)

        time.sleep(random.randint(1, 5))
        for _ in range(self.num_operations):
            if self.stop_:
                break
            self.do_something()

    def do_something(self):
        if random.uniform(0, 1) < 0.5: #Random behavior
                self.get_resource()
                self.perform_operation()
                self.release_resource()
    
    def perform_operation(self):
        time.sleep(2)  # Simulate resource usage

    def get_resource(self):   
        self.timestamp += 1
        self.log(f"[+]Processo {self.process_id} richiede la risorsa")
        self.queue.append({"timestamp": self.timestamp, "process_id": self.process_id})
        self.send_request()

        start = time.perf_counter()
        while self.acks_received.count(0) > 1 or (len(self.queue) > 0 and self.queue[0]["process_id"] != self.process_id):
            time.sleep(0.1)
            if self.log_file_path:
                with open(self.log_file_path.split('.')[0] + "_wait.txt", "a") as f:
                    if self.acks_received.count(0) > 1:
                        acks_missing = [i for i, ack in enumerate(self.acks_received) if ack == 0]
                        self.log(f"\tProcesso {self.process_id} in attesa di ACKs dei processi {acks_missing}")
                    else:
                        self.log(f"\tProcesso {self.process_id} in attesa di rilascio della risorsa da parte di {self.queue[0]['process_id']}")
            if self.stop_:
                self.send_release()
                return
        end = time.perf_counter()

        self.log(f"[+]Processo {self.process_id} ha acquisito la risorsa")
        self.n += 1

        if self.log_file_path:
            with open(self.log_file_path, "a") as f:
                f.write(f"Tempo di attesa del processo {self.process_id}: {end - start:.2f}\n")
    
    def release_resource(self):
        #assert self.queue[0]["process_id"] == self.process_id
        self.queue = [req for req in self.queue if req["process_id"] != self.process_id]
        self.log(f"[-]Processo {self.process_id} rilascia la risorsa")
        self.send_release()
        self.acks_received = [0 for _ in range(self.num_processes)]
        time.sleep(1)

    def accept_request(self):
        while not self.stop_:
            try:
                conn, addr = self.server_socket.accept()
            except OSError:
                break
            threading.Thread(target=self.handle_message, args=(conn,)).start()

    def handle_message(self, conn):
        while not self.stop_:
            data = conn.recv(1024).decode()
            try:
                message = json.loads(data)
            except json.JSONDecodeError:
                continue
            if "process_id" in message and message["process_id"] == self.process_id:
                continue
            if message["type"] == "request":
                self.timestamp = max(self.timestamp, message["timestamp"]) + 1
                self.queue.append({"timestamp": message["timestamp"], "process_id": message["process_id"]})
                self.queue.sort(key=lambda x: (x["timestamp"], x["process_id"]))
                self.send_ack(message["process_id"])
            elif message["type"] == "release":
                self.queue = [req for req in self.queue if req["process_id"] != message["process_id"]]
            elif message["type"] == "ack":
                self.log(f"[*] {self.process_id}: Ricevuto ack da {message['process_id']}.")
                self.acks_received[message["process_id"]] += 1

    def send_request(self):
        self.timestamp += 1
        message = json.dumps({"type": "request", "process_id": self.process_id, "timestamp": self.timestamp})
        for client_socket in self.client_sockets:
            if client_socket:
                client_socket.sendall(message.encode())

    def send_release(self):
        message = json.dumps({"type": "release", "process_id": self.process_id})
        for client_socket in self.client_sockets:
            if client_socket:
                client_socket.sendall(message.encode())

    def send_ack(self, process_id):
        message = json.dumps({"type": "ack", "process_id": self.process_id})
        self.log(f"Mando ack a {process_id}.")
        self.client_sockets[process_id].sendall(message.encode())

    def check_queue(self):
        while not self.stop_:
            with open("queue.txt", "a") as f:
                f.write(f"Processo {self.process_id} - Coda: {self.queue}\n")
            time.sleep(0.5)

    def acquire_resource(self):
        if self.log_file_path:
            with open(self.log_file_path, "a") as f:
                f.write(f"Process {self.process_id} acquired the resource\n")
        time.sleep(2)  # Simulate resource usage

    def stop(self):
        self.stop_ = True
        self.server_socket.close()
        self.server_thread.join()
        for client_socket in self.client_sockets:
            if client_socket:
                client_socket.close()

    def log(self, message):
        
        with open(self.log_file_path, "a") as f:
            f.write(message + "\n")