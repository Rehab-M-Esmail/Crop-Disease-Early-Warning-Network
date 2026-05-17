import paho.mqtt.client as mqtt
import json
import time

# Simulating a Zone 2 grape node processing updates
DEVICE_IDENTITY = "node_zone2_01"
AWS_MOCK_JOBS_TOPIC = f"$aws/things/{DEVICE_IDENTITY}/jobs/notify-next"
AWS_MOCK_REPLY_TOPIC = f"$aws/things/{DEVICE_IDENTITY}/jobs/+/update"

active_runtime_thresholds = {
    "humidity_trigger": 85.0,
    "temp_floor": 18.0,
    "temp_ceiling": 25.0
}

def on_cloud_connect(client, userdata, flags, rc, properties=None):
    print(f"[+] Edge Node {DEVICE_IDENTITY} verified and listening for Cloud Actions.")
    client.subscribe(AWS_MOCK_JOBS_TOPIC)

def process_ota_job(client, userdata, msg):
    try:
        envelope = json.loads(msg.payload.decode('utf-8'))
        execution = envelope.get("execution", {})
        target_job_id = execution.get("jobId")
        job_document = execution.get("jobDocument", {})
        
        if not target_job_id:
            return

        print(f"\n[!] ALERT: Processing job sequence '{target_job_id}'...")
        status_endpoint = AWS_MOCK_REPLY_TOPIC.replace("+", target_job_id)

        client.publish(status_endpoint, json.dumps({"status": "IN_PROGRESS"}), qos=1)
        time.sleep(1.5) 

        if "adjusted_thresholds" in job_document:
            global active_runtime_thresholds
            new_configs = job_document["adjusted_thresholds"]
            
            active_runtime_thresholds.update(new_configs)
            print(f"[✓] SUCCESS: Dynamic runtime parameters adjusted: {active_runtime_thresholds}")
            
            client.publish(status_endpoint, json.dumps({"status": "SUCCEEDED"}), qos=1)
        else:
            print("[-] FAILURE: Malformed payload specification detected. Rejecting transaction.")
            client.publish(status_endpoint, json.dumps({"status": "FAILED"}), qos=1)

    except Exception as e:
        print(f"[-] Critical system fault during firmware mutation cycle: {e}")

def run_ota_daemon():
    node_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    node_client.on_connect = on_cloud_connect
    node_client.on_message = process_ota_job

    node_client.connect("127.0.0.1", 1883, 60)
    node_client.loop_forever()

if __name__ == "__main__":
    run_ota_daemon()