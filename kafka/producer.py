import json
import sys
from kafka import KafkaProducer
import time

print("=" * 60)
print("Starting Kafka Producer...")
print("Connecting to Kafka at kafka:9092")
print("=" * 60)

try:
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict) else v.encode('utf-8'),
        retries=3
    )
    print("✅ Connected to Kafka successfully")
except Exception as e:
    print(f"❌ Failed to connect to Kafka: {e}", file=sys.stderr)
    sys.exit(1)

# Read the dirty data file
print("Reading from /data/events_dirty.jsonl...")
try:
    with open('/data/events_dirty.jsonl', 'r') as f:
        count = 0
        for line in f:
            line = line.strip()
            if line:
                try:
                    # Try to parse as JSON
                    data = json.loads(line)
                    producer.send('weather', value=data)
                    count += 1
                    if count % 100 == 0:
                        print(f"📤 Sent {count} records...")
                except json.JSONDecodeError:
                    # Send invalid JSON as string
                    producer.send('weather', value=line)
                    count += 1
                time.sleep(0.05)  # Small delay between records
        
        print(f"✅ Finished sending {count} total records")
except FileNotFoundError:
    print("❌ Error: /data/events_dirty.jsonl not found", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"❌ Error reading file: {e}", file=sys.stderr)
    sys.exit(1)

print("Flushing producer...")
producer.flush()
producer.close()
print("✅ Producer closed successfully")

