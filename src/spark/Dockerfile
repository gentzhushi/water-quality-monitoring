FROM spark:3.5.2-scala2.12-java11-python3-ubuntu

WORKDIR /app

USER root
RUN mkdir -p /tmp/spark-ivy && chmod -R 777 /tmp/spark-ivy

COPY streaming_job.py .

USER spark

ENTRYPOINT ["/opt/spark/bin/spark-submit", "--conf", "spark.jars.ivy=/tmp/spark-ivy", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2", "/app/streaming_job.py"]
