import paho.mqtt.client as mqtt
import json
import time

LOCAL_BROKER_HOST = "127.0.0.1"
LOCAL_BROKER_PORT = 1883
CHIRPSTACK_UPLINK_TOPIC = "application/+/device/+/event/up"

MOCK_CLOUD_HOST = "127.0.0.1"
MOCK_CLOUD_PORT = 1883 

def on_local_connect(client, userdata, flags, rc, properties=None):
    print(f"[+] Operational connection established with ChirpStack broker. Code: {rc}")
    client.subscribe(CHIRPSTACK_UPLINK_TOPIC)

def on_message_received(client, userdata, msg):
    try:
        raw_payload = json.loads(msg.payload.decode('utf-8'))
        
        device_name = raw_payload.get("deviceInfo", {}).get("deviceName")
        tags = raw_payload.get("deviceInfo", {}).get("tags", {})
        zone_context = tags.get("zone", "Zone_Unknown") 
        
        decoded_object = raw_payload.get("object", {})
        
        if decoded_object:
            normalized_payload = {
                "device_id": device_name,
                "zone": zone_context,
                "timestamp": int(time.time()),
                "temperature": float(decoded_object.get("temperature", 0)),
                "humidity": float(decoded_object.get("humidity", 0)),
                "leaf_wetness": int(decoded_object.get("leaf_wetness", 0)),
                "rainfall": float(decoded_object.get("rainfall", 0))
            }
            
            target_cloud_topic = f"agriculture/field_telemetry/{device_name}"
            
            userdata['cloud_client'].publish(target_cloud_topic, json.dumps(normalized_payload), qos=1)
            print(f"[Forwarded Frame] -> Topic: {target_cloud_topic} | Data: {normalized_payload}")
            
    except Exception as err:
        print(f"[-] Processing abnormality encountered during message pass: {err}")

def initialize_bridge_pipeline():
    cloud_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    cloud_client.connect(MOCK_CLOUD_HOST, MOCK_CLOUD_PORT, 60)
    cloud_client.loop_start()

    bridge_handler = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, userdata={'cloud_client': cloud_client})
    bridge_handler.on_connect = on_local_connect
    bridge_handler.on_message = on_message_received

    bridge_handler.connect(LOCAL_BROKER_HOST, LOCAL_BROKER_PORT, 60)
    print("[*] Cloud Integration Bridge Active. Awaiting local frame streams...")
    bridge_handler.loop_forever()

if __name__ == "__main__":
    initialize_bridge_pipeline()