import json
import time
import random
import paho.mqtt.client as mqtt
from datetime import datetime
import struct
import base64

CHIRPSTACK_MQTT_HOST = "localhost"
CHIRPSTACK_MQTT_PORT = 1883
REGION_PREFIX = "eu868"  

NODES = [
    {"id": "node_zone1_01", "zone": 1, "dev_eui": "0000000000000001", "dev_addr": "01000001"},
    {"id": "node_zone1_02", "zone": 1, "dev_eui": "0000000000000002", "dev_addr": "01000002"},
    {"id": "node_zone1_03", "zone": 1, "dev_eui": "0000000000000003", "dev_addr": "01000003"},
    {"id": "node_zone1_04", "zone": 1, "dev_eui": "0000000000000004", "dev_addr": "01000004"},
    {"id": "node_zone1_05", "zone": 1, "dev_eui": "0000000000000005", "dev_addr": "01000005"},
    {"id": "node_zone2_01", "zone": 2, "dev_eui": "0000000000000011", "dev_addr": "02000001"},
    {"id": "node_zone2_02", "zone": 2, "dev_eui": "0000000000000012", "dev_addr": "02000002"},
    {"id": "node_zone2_03", "zone": 2, "dev_eui": "0000000000000013", "dev_addr": "02000003"},
    {"id": "node_zone2_04", "zone": 2, "dev_eui": "0000000000000014", "dev_addr": "02000004"},
    {"id": "node_zone2_05", "zone": 2, "dev_eui": "0000000000000015", "dev_addr": "02000005"},
]

def get_realistic_data(step):
    step_mod = step % 36
    
    if step_mod < 12:
        temp = random.uniform(10, 14)
        humidity = random.uniform(50, 68)
        leaf_wetness = random.uniform(0, 2)
        rainfall = random.uniform(0, 1)
    elif step_mod < 24:
        progress = (step_mod - 12) / 12.0
        temp = random.uniform(18, 24)
        humidity = 70 + (progress * 18) + random.uniform(-2, 2)
        leaf_wetness = progress * 6 + random.uniform(0, 1)
        rainfall = random.uniform(0, 2)
    else:
        temp = random.uniform(19, 23)
        humidity = random.uniform(88, 97)
        leaf_wetness = random.uniform(8, 12)
        rainfall = random.uniform(6, 15)
    
    return {
        "temperature": round(temp, 1),
        "humidity": round(humidity, 1),
        "leaf_wetness": round(leaf_wetness, 1),
        "rainfall": round(rainfall, 2)
    }

def encode_payload(data):
    """Encode sensor data as base64 for LoRaWAN"""
    payload = struct.pack(
        ">hHHH",
        int(data["temperature"] * 10),
        int(data["humidity"] * 10),
        int(data["leaf_wetness"] * 10),
        int(data["rainfall"] * 100)
    )
    return base64.b64encode(payload).decode()

def build_gateway_uplink(node, data, fcnt):
    """Build a properly formatted gateway uplink frame that ChirpStack will accept"""
    payload_bytes = struct.pack(
        ">hHHH",
        int(data["temperature"] * 10),
        int(data["humidity"] * 10),
        int(data["leaf_wetness"] * 10),
        int(data["rainfall"] * 100)
    )
    
    mhdr = bytes([0x40]) 
    dev_addr = bytes.fromhex(node['dev_addr'])
    fctrl = bytes([0x00]) 
    fcnt_bytes = fcnt.to_bytes(2, 'big')
    fport = bytes([0x01])
    
    mac_payload = dev_addr + fctrl + fcnt_bytes + fport + payload_bytes
    full_payload = mhdr + mac_payload
    

    dummy_mic = bytes([0x00, 0x00, 0x00, 0x00])
    phy_payload = full_payload + dummy_mic
    
    return {
        "phyPayload": base64.b64encode(phy_payload).decode(),
        "txInfo": {
            "frequency": 868100000,
            "modulation": "LORA",
            "loRaModulationInfo": {
                "bandwidth": 125,
                "spreadingFactor": 7,
                "codeRate": "4/5"
            }
        },
        "rxInfo": {
            "gatewayId": f"000000000000000{node['zone']}",
            "rssi": random.randint(-80, -50),
            "loRaSNR": round(random.uniform(5, 12), 1),
            "channel": 0,
            "rfChain": 0
        }
    }

def main():
    client = mqtt.Client()
    client.connect(CHIRPSTACK_MQTT_HOST, CHIRPSTACK_MQTT_PORT, 60)
    client.loop_start()
    
    time.sleep(1)
    
    print(f"Starting simulation for {len(NODES)} devices...")
    print(f"Publishing to gateway topics with region prefix: {REGION_PREFIX}")
    print("=" * 60)
    
    step = 0
    fcnt = {node['dev_addr']: 0 for node in NODES}
    
    try:
        while True:
            for node in NODES:
                data = get_realistic_data(step)
                fcnt[node['dev_addr']] += 1
                
                frame = build_gateway_uplink(node, data, fcnt[node['dev_addr']])
                
                topic = f"{REGION_PREFIX}/gateway/000000000000000{node['zone']}/event/up"
                client.publish(topic, json.dumps(frame), qos=1)
                
                print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                      f"Zone {node['zone']}: {node['id']} | "
                      f"T:{data['temperature']}°C H:{data['humidity']}% | "
                      f"FCnt:{fcnt[node['dev_addr']]}")
            
            print("-" * 60)
            time.sleep(5)
            step += 1
            
    except KeyboardInterrupt:
        print("\nSimulation stopped")
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()