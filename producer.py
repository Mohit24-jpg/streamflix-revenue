import time
import json
import random
from kafka import KafkaProducer
from faker import Faker

# 1. Connect to our Redpanda Container
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

fake = Faker()
print("🎬 Netflix Revenue Stream Started... Press Ctrl+C to stop.")

# 2. Infinite Loop: Generate Fake Subscriptions
try:
    while True:
        event = {
            "event_id": fake.uuid4(),
            "event_type": random.choice(['signup', 'upgrade', 'cancel']),
            "user_country": fake.country_code(),
            "amount": random.choice([9.99, 15.49, 19.99]),
            "timestamp": int(time.time())
        }
        
        # Send data to Redpanda
        producer.send('subscription_events', event)
        print(f"Sent: {event['event_type']} from {event['user_country']}")
        
        # Speed limit: 1 event per second (we will speed this up later)
        time.sleep(1)

except KeyboardInterrupt:
    print("\n🛑 Stream stopped.")