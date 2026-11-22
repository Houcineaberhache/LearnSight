FROM apache/airflow:2.8.1-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow
RUN pip install --no-cache-dir "apache-airflow==2.8.1" apache-airflow-providers-apache-spark \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt"
RUN pip install --no-cache-dir pyspark==3.5.1