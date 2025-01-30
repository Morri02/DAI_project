import random
import os
import json
import time
from multiprocessing import Process
from threading import Thread
import paho.mqtt.client as mqtt


file_path = "config.json"
with open(file_path, "r") as config_file:
    d = json.load(config_file)


TOPIC_REQUESTS = d["request_topic"]
TOPIC_RELEASES = d["releases_topic"]
TOPIC_ACKS = d["ack_topic"]
TOPIC_HEART_BEAT = d["heart_beat_topic"]

class LamportProcessMQTT(Thread):
    def __init__(self, process_id, num_process, broker="localhost", port=1883, log_file_path=None):
        super().__init__()
        self.process_id = process_id
        self.num_processes = num_process
        self.timestamp = 0
        self.queue = []  # Lista di dizionari: {"timestamp": int, "process_id": int}
        self.acks_received = [0 for _ in range(self.num_processes)]

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=str(process_id))
        self.client.on_message = self.on_message
        #self.client.on_connect = self.on_connect
        #self.client.on_disconnect = self.on_disconnect
        self.client.connect(broker, port, 60)
        self.client.subscribe([(TOPIC_REQUESTS, 0), (TOPIC_RELEASES, 0), (f"{TOPIC_ACKS}{self.process_id}", 0), (f"{TOPIC_HEART_BEAT}{self.process_id}", 0)])
        self.client.loop_start()

        self.stop_ = False
        self.num_operations = random.randint(5, 10)

        self.log_file_path = log_file_path
        self.n = 0 #Number of time it acquired the resource

        self.t = Thread(target=self.check_queue)
        with open("queue.txt", "w") as f:
            f.write(f"Processo{self.process_id} - Coda: {self.queue}\n")
        self.t.start()

    def check_queue(self):
        while not self.stop_:
            with open("queue.txt", "a") as f:
                f.write(f"Processo {self.process_id} - Coda: {self.queue}\n")
            time.sleep(0.5)

    def run(self):
        time.sleep(random.randint(1, 5))
        for _ in range(self.num_operations):
            if self.stop_:
                break
            self.do_something()

    def stop(self):
        self.stop_ = True
        self.t.join()

    def do_something(self):
        if random.uniform(0, 1) < 0.5: #Random behavior
                self.get_resource()
                self.perform_operation()
                self.release_resource()
    
    def perform_operation(self):
        time.sleep(2)

    def on_connect(self, client, userdata, flags, rc, properties):
        print(f"Processo {self.process_id} connesso al broker con codice di ritorno {rc}")

    def on_disconnect(self, client, userdata, flags, rc, properties):
        print(f"Processo {self.process_id} disconnesso dal broker con codice di ritorno {rc}")

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        if topic.startswith(TOPIC_HEART_BEAT):
            if topic.startswith(f"{TOPIC_HEART_BEAT}{self.process_id}"):
                self.client.publish(f"{TOPIC_HEART_BEAT}{self.process_id}", "yes", qos=2)
            return
        
        payload = json.loads(msg.payload)

        if topic == TOPIC_REQUESTS:
            if payload["process_id"] != self.process_id:
                self.timestamp = max(self.timestamp, payload['timestamp']) + 1
                self.queue.append({"timestamp": payload['timestamp'], "process_id": payload['process_id']})
                self.queue.sort(key=lambda x: (x["timestamp"], x["process_id"]))  # Ordina per timestamp e process_id
                self.client.publish(f"{TOPIC_ACKS}{payload['process_id']}", json.dumps({"ack_from": self.process_id}), qos=2)

        elif topic == TOPIC_RELEASES:
            # Togli solo prima occorrenza
            # try:
            #     self.queue.remove({"process_id": payload['process_id']})
            # except ValueError:
            #     pass

            self.queue = [req for req in self.queue if req["process_id"] != payload['process_id']]
            self.log(f"[+] {self.process_id}: Processo {payload['process_id']} ha rilasciato la risorsa")

        elif topic.startswith(f"{TOPIC_ACKS}{self.process_id}"):
            self.acks_received[payload['ack_from']] += 1

    def send_request(self):
        self.client.publish(TOPIC_REQUESTS, json.dumps({"process_id": self.process_id, "timestamp": self.timestamp}))


    def get_resource(self):
        self.timestamp += 1
        self.queue.append({"timestamp": self.timestamp, "process_id": self.process_id})
        self.queue.sort(key=lambda x: (x["timestamp"], x["process_id"]))
        self.send_request()
        time.sleep(1)
        self.log(f"[+]Processo {self.process_id} richiede la risorsa, timestamp: {self.timestamp}")

        start = time.perf_counter()
        while self.acks_received.count(0) > 1 or (len(self.queue) > 0 and self.queue[0]["process_id"] != self.process_id):
            time.sleep(0.5)
            if self.log_file_path:
                with open(self.log_file_path.split('.')[0] + "_wait.txt", "a") as f:
                    if self.acks_received.count(0) > 1:
                        acks_missing = [i for i, ack in enumerate(self.acks_received) if ack == 0]
                        self.log(f"\tProcesso {self.process_id} in attesa di ACKs dei processi {acks_missing}")
                    else:
                        self.log(f"\tProcesso {self.process_id} in attesa di rilascio della risorsa da parte di {self.queue[0]['process_id']}")
            if self.stop_:
                self.release_resource()
                return
        end = time.perf_counter()

        self.log(f"[+]Processo {self.process_id} ha acquisito la risorsa")
        self.n += 1

        if self.log_file_path:
            with open(self.log_file_path, "a") as f:
                f.write(f"Tempo di attesa del processo {self.client._client_id}: {end - start:.2f}\n")
        
    def release_resource(self):
        #assert self.queue[0]["process_id"] == self.process_id
        self.queue = [req for req in self.queue if req["process_id"] != self.process_id]
        self.log(f"[-]Processo {self.process_id} rilascia la risorsa")
        self.client.publish(TOPIC_RELEASES, json.dumps({"process_id": self.process_id}), qos=2)
        self.acks_received = [0 for _ in range(self.num_processes)]
        time.sleep(1)

    def log(self, message):
        with open(self.log_file_path, "a") as f:
            f.write(message + "\n")
