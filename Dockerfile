FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

ARG SPARK_VERSION="3.1.2"
ARG HADOOP_VERSION="3.2"


# default credentials: login - airflow, password - airflow

USER root
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y gnupg2 && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB9B1D8886F44E2A && \
    add-apt-repository "deb http://security.debian.org/debian-security stretch/updates main" && \
    apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    pip freeze && \
    java -version $$ \
    javac -version


# Setup JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN export JAVA_HOME

ENV SPARK_HOME /usr/local/spark
RUN apt-get install wget -y
# Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
RUN cd "/tmp" && \
        wget --no-verbose "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        mkdir -p "${SPARK_HOME}/bin" && \
        mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
        rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
# Create SPARK_HOME env var
RUN export SPARK_HOME
ENV PATH $PATH:/usr/local/spark/bin
ENV PATH $PATH:/root/.local/bin

EXPOSE 8080 5555 8793

RUN apt-get update -qq && apt-get install vim -qqq

COPY requirements.txt .
# RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR $AIRFLOW_HOME

COPY dags dags
COPY spark core
COPY spark dags/core
COPY spark /usr/local/spark/core

COPY example example
COPY example /usr/local/spark/example

COPY csv csv
COPY csv /usr/local/spark/csv

COPY resources resources
COPY resources /usr/local/spark/resources

COPY data_generate_scripts data_generate_scripts
COPY data_generate_scripts /usr/local/spark/data_generate_scripts

COPY scripts scripts
RUN chmod +x scripts
