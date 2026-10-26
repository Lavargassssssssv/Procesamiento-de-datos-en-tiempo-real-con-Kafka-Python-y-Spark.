Procesamiento de datos en tiempo real con Kafka y Spark

1. Descripción del proyecto

Este proyecto implementa un sistema de procesamiento de datos en tiempo real utilizando Python, Apache Kafka y Apache Spark Structured Streaming.

El sistema simula sensores que generan datos de temperatura y humedad. Los registros son enviados a Kafka y posteriormente procesados por Spark para calcular promedios por sensor en ventanas de tiempo de un minuto.

2. Objetivo

Implementar un flujo de datos que permita generar, transmitir, procesar y analizar información simulada de sensores utilizando tecnologías de procesamiento distribuido y streaming.

3. Tecnologías utilizadas

Python 3

Apache Kafka

Apache Spark 3.5.8

PySpark

kafka-python

4. Archivos del repositorio

kafka_producer.py: genera datos simulados de sensores y los envía al tema datos_sensores.

spark_streaming_consumer.py: lee los mensajes de Kafka, interpreta los registros y calcula promedios de temperatura y humedad por sensor en ventanas de un minuto.

5. Estructura de los datos

Cada registro contiene:

sensor_id: identificador del sensor.

temperature: temperatura simulada.

humidity: humedad simulada.

timestamp: fecha y hora representada mediante una marca de tiempo Unix.

6. Requisitos

Python 3.

Apache Kafka configurado y en ejecución.

Apache Spark 3.5.8.

Biblioteca Python kafka-python.

Conector de Spark para Kafka compatible con Spark 3.5.8 y Scala 2.12.

7. Instrucciones de ejecución

Iniciar ZooKeeper o el servicio de coordinación configurado para la instalación de Kafka.

Iniciar el servidor Kafka.

Crear el tema si todavía no existe:

/opt/Kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic datos_sensores --partitions 1 --replication-factor 1

Iniciar el productor:

python3 /home/vboxuser/kafka_producer.py

En otra terminal, ejecutar el consumidor con el conector de Kafka:

/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 /home/vboxuser/spark_streaming_consumer.py

El comando supone que las dependencias del conector están disponibles o pueden descargarse. Si ya se descargaron localmente, puede utilizarse la configuración de dependencias correspondiente al entorno.

8. Resultados esperados

El productor genera registros continuamente. Spark consume los mensajes de Kafka y presenta en la consola los promedios de temperatura y humedad agrupados por sensor y por ventanas de un minuto.

La interfaz web de Spark, disponible normalmente en el puerto 4040 mientras la aplicación está activa, permite supervisar trabajos y etapas del procesamiento.

9. Conclusión

La implementación permite observar la integración entre la generación de datos con Python, la transmisión mediante Kafka y el procesamiento de flujos con Spark Structured Streaming. Esta arquitectura facilita el análisis continuo de datos simulados y permite comprender el funcionamiento de un flujo de procesamiento en tiempo real.
