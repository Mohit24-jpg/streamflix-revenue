import time
import json
import random
from kafka import KafkaProducer

# --- CONFIGURATION ---
TOPIC = "subscription_events"
BROKERS = ["redpanda:29092"]

# --- FAKE DATA GENERATORS ---
# We use realistic weights so the chart looks organic (mostly US/BR, some others)
COUNTRIES = ["US", "US", "US", "BR", "BR", "GB", "DE", "FR", "JP", "IN", "CA"]
EVENTS = ["signup", "signup", "upgrade", "cancel", "renew"]

def make_producer():
    # Wait for Redpanda to start
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except Exception:
            print("⏳ Waiting for Redpanda...")
            time.sleep(2)

def generate_event():
    """Generates a realistic Netflix-style event."""
    return {
        "event_type": random.choice(EVENTS),
        # Generate a fake UUID like 'user-1023'
        "user_id": f"user-{random.randint(1000, 9999)}",
        # Randomize price slightly
        "amount": round(random.choice([9.99, 14.99, 19.99]) + random.uniform(-0.5, 0.5), 2),
        # The field missing before:
        "country": random.choice(COUNTRIES),
        "timestamp": time.time()
    }

if __name__ == "__main__":
    producer = make_producer()
    print(f"🚀 Streamflix Producer Online. Targeting topic: {TOPIC}")
    
    try:
        while True:
            event = generate_event()
            producer.send(TOPIC, event)
            print(f"Sent: {event['event_type']} | {event['country']} | ${event['amount']}")
            # Fast speed for the demo (10 events per second)
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\n🛑 Simulation Stopped.")