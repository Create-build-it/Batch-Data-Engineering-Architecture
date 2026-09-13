FROM bitnami/spark:3.5

USER root
WORKDIR /app

RUN curl -L -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

RUN pip install --no-cache-dir psycopg2-binary==2.9.9

COPY spark_job.py .

CMD ["spark-submit", "--master", "local[*]", \
     "--jars", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar", \
     "/app/spark_job.py"]
