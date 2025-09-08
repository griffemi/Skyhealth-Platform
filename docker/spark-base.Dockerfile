FROM bitnami/spark:4.0.1
USER root
RUN mkdir -p /opt/spark/jars && \
    curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.12/1.5.0/iceberg-spark-runtime-4.0_2.12-1.5.0.jar -o /opt/spark/jars/iceberg-runtime.jar
ENV SPARK_NO_DAEMONIZE=true
ENTRYPOINT ["/opt/bitnami/scripts/spark/entrypoint.sh"]
CMD ["/opt/bitnami/scripts/spark/run.sh"]
