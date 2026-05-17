import time
import json
import random
import threading
from datetime import datetime, timezone
from awscrt import mqtt
from awsiot import mqtt_connection_builder

# ─── CONFIG ────────────────────────────────────────────────────────────────────
ENDPOINT = "ae4y2psgbquml-ats.iot.us-east-1.amazonaws.com"  
PORT     = 8883
INTERVAL = 5   

DEVICES = [
    {
        "id":   "node_zone1_01",
        "cert": "sensor1/certificate.pem.crt",
        "key":  "sensor1/private.pem.key",
        "ca":   "sensor1/AmazonRootCA1.pem",
    },
    {
        "id":   "node_zone1_02",
        "cert": "sensor2/certificate.pem.crt",
        "key":  "sensor2/private.pem.key",
        "ca":   "sensor2/AmazonRootCA1.pem",
    },
    {
        "id":   "node_zone2_01",
        "cert": "sensor3/certificate.pem.crt",
        "key":  "sensor3/private.pem.key",
        "ca":   "sensor3/AmazonRootCA1.pem",
    },
    {
        "id":   "node_zone2_02",
        "cert": "sensor4/certificate.pem.crt",
        "key":  "sensor4/private.pem.key",
        "ca":   "sensor4/AmazonRootCA1.pem",
    },
]

# ─── REALISTIC DATA GENERATOR ──────────────────────────────────────────────────
def generate_telemetry(device_id):
    """Generate realistic crop disease risk sensor readings."""
    seed = int(device_id[-2:])  # 01 → 1, 02 → 2, 03 → 3
    return {
        "device_id":    device_id,
        "timestamp":    int(datetime.now(timezone.utc).timestamp()),  # Unix epoch (int)
        "temperature":  round(random.uniform(18.0 + seed, 35.0 + seed), 2),  # °C
        "humidity":     round(random.uniform(55.0, 98.0),              2),   # %
        "leaf_wetness": round(random.uniform(0,    10),                1),   # 0–10 scale
        "rainfall":     round(random.uniform(0.0,  15.0),              2),   # mm
    }

# ─── SENSOR THREAD ─────────────────────────────────────────────────────────────
def run_sensor(device):
    device_id = device["id"]
    topic = f"crop/telemetry/{device_id}"

    print(f"[{device_id}] Connecting to AWS IoT...")

    connection = mqtt_connection_builder.mtls_from_path(
        endpoint=ENDPOINT,
        port=PORT,
        cert_filepath=device["cert"],
        pri_key_filepath=device["key"],
        ca_filepath=device["ca"],
        client_id=device_id,
        clean_session=True,
        keep_alive_secs=30,
    )

    connect_future = connection.connect()
    connect_future.result()   # blocks until connected
    print(f"[{device_id}] ✅ Connected. Publishing to '{topic}' every {INTERVAL}s")

    try:
        while True:
            payload = generate_telemetry(device_id)
            connection.publish(
                topic=topic,
                payload=json.dumps(payload),
                qos=mqtt.QoS.AT_LEAST_ONCE,
            )
            print(f"[{device_id}] 📤 {payload}")
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        pass
    finally:
        print(f"[{device_id}] Disconnecting...")
        disconnect_future = connection.disconnect()
        disconnect_future.result()
        print(f"[{device_id}] Disconnected.")

# ─── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    threads = []
    for device in DEVICES:
        t = threading.Thread(target=run_sensor, args=(device,), daemon=True)
        threads.append(t)
        t.start()
        time.sleep(1)  

    print("\n🚀 All 3 sensors running. Press Ctrl+C to stop.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n⛔ Stopping all sensors...")