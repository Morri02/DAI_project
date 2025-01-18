from process_MQTT import LamportProcessMQTT
from process_socket import LamportProcessTCP
import time
import random
import json
from threading import Thread
import paho.mqtt.client as mqtt

file_path = "config.json"
with open(file_path, "r") as config_file:
    d = json.load(config_file)

BROKER = d["broker"]
PORT = d["port"]
TOPIC_REQUESTS = d["request_topic"]
TOPIC_RELEASES = d["releases_topic"]
TOPIC_ACKS = d["ack_topic"]
TOPIC_HEART_BEAT = d["heart_beat_topic"]
NUM_PROCESS = d["num_processes"]

class Coordinator(Thread):
    def __init__(self):
        super().__init__()
        self.num_processes = NUM_PROCESS
        self.queue = []  # Lista di dizionari: {"timestamp": int, "process_id": int}
        self.alive_processes = [False for _ in range(self.num_processes)]
        self.dead_processes = [False for _ in range(self.num_processes)]

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="observer")
        self.client.on_message = self.on_message
        self.client.connect(BROKER, PORT, 60)
        self.client.subscribe([(TOPIC_REQUESTS, 0), (TOPIC_RELEASES, 0)])
        self.client.loop_start()

        self.stop_ = False
        self.empty_queue = 0 #Number of consecutive times the queue was empty


    def on_message(self, client, userdata, message):
        topic = message.topic
        payload = message.payload.decode("utf-8")
        if topic == TOPIC_REQUESTS:
            self.queue.append(json.loads(payload))
            self.queue.sort(key=lambda x: (x["timestamp"], x["process_id"]))
        elif topic == TOPIC_RELEASES:
            self.queue = [elem for elem in self.queue if elem["process_id"] != json.loads(payload)["process_id"]]
        elif topic == f"{TOPIC_HEART_BEAT}+":
            process_id = int(topic.split("/")[-1])
            self.alive_processes[process_id] = True
            
    
    def stop(self):
        self.stop_ = True
    
    def run(self):
        while not self.stop_:
            self.check_queue()
            #self.check_processes()
            time.sleep(2)
    
    def check_queue(self):
        print("Lunghezza coda:", len(self.queue))
        if not self.queue:
            self.empty_queue += 1
        else:
            self.empty_queue = 0
        if self.empty_queue >= 10:
            self.stop()
            print("Observer stopped.")
            return
    
    def check_processes(self):
        threads = []
        self.alive_processes = [False for _ in range(self.num_processes)]
        for process_id in range(self.num_processes):
            t = Thread(target=self.check_process, args=(process_id,))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()
        print(f"Alive processes: {[i for i, alive in enumerate(self.alive_processes) if alive]}")
    

    def check_process(self, process_id):
        self.client.publish(f"{TOPIC_HEART_BEAT}{process_id}", "Alive?")
        times = 0
        while not self.alive_processes[process_id]:
            time.sleep(1)
            times += 1
            if times >= 5:
                self.dead_processes[process_id] = True
                break

class LamportMutex():
    def __init__(self):
        #self.processes_MQTT = [LamportProcessMQTT(i, NUM_PROCESS, broker=BROKER, port=PORT, log_file_path=f"log_files/MQTT{i}.log") for i in range(NUM_PROCESS)]
        #self.observer = Coordinator()
        self.processes_socket = [LamportProcessTCP(i, NUM_PROCESS, log_file_path=f"log_files/socket{i}.log") for i in range(NUM_PROCESS)]
    
    def start(self):
        start = time.time()
        #self.observer.start()
        for process in self.processes_socket:
            print("Starting process", process.process_id)
            process.start()
        
        # if self.observer.is_alive():
        #     self.observer.join()
        #     print("Stopping processes")
        #     for process in self.processes:
        #         process.stop()
        #         process.join()
        
        for process in self.processes_socket:
            process.join()
            process.stop()

        for process in self.processes_socket:
            n = process.n
            print(f"Process {process.process_id} acquired the resource {n} times out of the {process.num_operations} possible.")
        
        end = time.time()
        elapsed_time = end - start
        print(f"Execution time:{elapsed_time} seconds ({elapsed_time/60} minutes)")

   
if __name__ == "__main__":
    mutex = LamportMutex()
    
    mutex.start()
