FROM apache/spark:3.5.3

USER root

RUN mkdir -p /opt/spark/jobs

COPY /hello_spark.py /opt/spark/jobs/

RUN chown -R spark:spark /opt/spark/jobs

USER spark