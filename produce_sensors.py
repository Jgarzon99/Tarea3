from kafka import KafkaProducer
import csv, json, time, random
from datetime import datetime, timezone

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

topic = 'traffic-readings'
csv_file = 'traffic_data.csv'

def send_row(row):
    # ensure types
    row['vehicle_count'] = int(row['vehicle_count'])
    row['avg_speed'] = float(row['avg_speed'])
    producer.send(topic, row)
    producer.flush()

if __name__ == '__main__':
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                send_row(row)
                # controla la frecuencia de envío (simulación)
                time.sleep(0.5)
    except KeyboardInterrupt:
        print('Deteniendo productor...')
    finally:
        producer.flush()
