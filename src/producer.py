# Databricks notebook source
import json, uuid, time, random, os
from datetime import datetime, timezone
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

WORKSPACE_ID    = "2281745829657864"
WORKSPACE_URL   = "https://abcd-teste2-test-spcse2.cloud.databricks.com"
REGION          = "us-east-1"
SERVER_ENDPOINT = f"https://{WORKSPACE_ID}.zerobus.{REGION}.cloud.databricks.com"
TARGET_TABLE    = "dltvijay.zerobus.sensor_readings"

CLIENT_ID     = os.environ["DATABRICKS_CLIENT_ID"]
CLIENT_SECRET = os.environ["DATABRICKS_CLIENT_SECRET"]

sdk = ZerobusSdk(
    server_endpoint=SERVER_ENDPOINT,
    workspace_url=WORKSPACE_URL,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
)

stream = sdk.open_stream(
    TableProperties(TARGET_TABLE),
    StreamConfigurationOptions(record_type=RecordType.JSON)
)

print("📡 Sending 10 dummy sensor readings...")

for i in range(10):
    reading = {
        "event_id":    str(uuid.uuid4()),
        "sensor_id":   random.choice(["SENS-001", "SENS-002", "SENS-003"]),
        "temperature": round(random.uniform(15.0, 95.0), 2),
        "pressure":    round(random.uniform(28.0, 32.0), 2),
        "status":      random.choice(["normal", "warning", "critical"]),
        "event_time":  datetime.now(timezone.utc).isoformat()
    }
    stream.ingest(json.dumps(reading))
    print(f"[{i+1}/10] 📤 {reading['sensor_id']} | {reading['temperature']}°C | {reading['status']}")
    time.sleep(2)

stream.close()
print("✅ Done! 10 records sent.")