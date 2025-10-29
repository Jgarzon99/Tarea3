# Proyecto Ejecutable — Spark + Kafka Traffic Sensors

## Contenido
- docker-compose.yml  -> Kafka + Zookeeper
- requirements.txt
- produce_sensors.py   -> Productor que lee traffic_data.csv y envía mensajes a Kafka
- batch_etl.py        -> Job PySpark batch para limpieza y agregación
- streaming_app.py    -> Spark Structured Streaming consumer (Kafka -> agregados por ventana)
- dashboard.py        -> Dash app que muestra resultados (lectura Parquet)
- traffic_data.csv    -> Dataset de ejemplo (100 registros)

## Instrucciones rápidas
1. Instala dependencias:
   ```bash
   pip install -r requirements.txt
   ```

2. Levanta Kafka y Zookeeper:
   ```bash
   docker-compose up -d
   ```

3. Crea el topic (si no existe):
   ```bash
   docker exec -it kafka kafka-topics --create --topic traffic-readings --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   ```

4. Ejecuta el productor (envía filas del CSV):
   ```bash
   python produce_sensors.py
   ```

5. Ejecuta Spark Structured Streaming (desde el directorio con Spark instalado):
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 streaming_app.py
   ```

6. Ejecuta job batch:
   ```bash
   spark-submit batch_etl.py
   ```

7. Levanta dashboard:
   ```bash
   python dashboard.py
   ```

## Notas
- Ajusta rutas si corres desde otro directorio.
- Aumenta la tasa en produce_sensors.py para simular mayor volumen.
