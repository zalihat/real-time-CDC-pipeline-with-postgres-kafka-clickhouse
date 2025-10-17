from confluent_kafka import Consumer, KafkaException
import json
import requests
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# ========== TELEGRAM CONFIG ==========
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TELEGRAM_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

# ========== KAFKA CONFIG ==========
conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'alert-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topic = "debezium.public.sales_test"
consumer.subscribe([topic])

def send_telegram_alert(message: str):
    """Send a message to Telegram."""
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        requests.post(TELEGRAM_URL, json=payload)
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")

def format_alert(data):
    """Create an alert text based on the Debezium event."""
    op = data.get("op")
    before = data.get("before")
    after = data.get("after")

    if op == "c":
        msg = f"🟢 *New Record Added*\nCustomer: {after['customer_name']}\nItem: {after['item']}\nAmount: {after['amount']}"
    elif op == "u":
        changes = []
        for key in after.keys():
            if before and before.get(key) != after.get(key):
                changes.append(f"🔸 {key}: {before.get(key)} → {after.get(key)}")
        changes_text = "\n".join(changes) if changes else "(No field changes detected)"
        msg = f"🟡 *Record Updated*\nID: {after['id']}\n{changes_text}"
    elif op == "d":
        msg = f"🔴 *Record Deleted*\nCustomer: {before['customer_name']}\nItem: {before['item']}"
    else:
        msg = f"ℹ️ Unknown operation: {op}"

    return msg

print(f"Listening for messages on topic '{topic}'...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        try:
            event = json.loads(msg.value().decode("utf-8"))
            payload = event.get("payload", {})

            alert_text = format_alert(payload)
            print("\n" + alert_text)  # print locally
            send_telegram_alert(alert_text)  # send to Telegram

        except json.JSONDecodeError:
            print("Received non-JSON message:", msg.value())

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
