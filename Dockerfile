FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

ARG SPARK_VERSION="3.1.2"
# ARG HADOOP_VERSION="3.2"

# Setup JAVA_HOME
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


ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN export JAVA_HOME

# INSTALL APACHE HADOOP
# ENV JAVA_HOME "/usr/lib/jvm/java-11-openjdk-amd64"
ARG HADOOP_VERSION="3.2.1"
ENV HADOOP_HOME "/opt/hadoop"
RUN curl https://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    | tar xz -C /opt && mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}
ENV HADOOP_COMMON_HOME "${HADOOP_HOME}"
ENV HADOOP_CLASSPATH "${HADOOP_HOME}/share/hadoop/tools/lib/*"
ENV HADOOP_CONF_DIR "${HADOOP_HOME}/etc/hadoop"
ENV PATH "$PATH:${HADOOP_HOME}/bin"
ENV HADOOP_OPTS "$HADOOP_OPTS -Djava.library.path=${HADOOP_HOME}/lib"
ENV HADOOP_COMMON_LIB_NATIVE_DIR "${HADOOP_HOME}/lib/native"
ENV YARN_CONF_DIR "${HADOOP_HOME}/etc/hadoop"


# Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
# Create SPARK_HOME env var
ARG SPECIAL_SPARK_HADOOP="3.2"
ENV SPARK_HOME /usr/local/spark
RUN apt-get install wget -y
RUN cd "/tmp" && \
        wget --no-verbose "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${SPECIAL_SPARK_HADOOP}.tgz" && \
        tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${SPECIAL_SPARK_HADOOP}.tgz" && \
        mkdir -p "${SPARK_HOME}/bin" && \
        mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${SPECIAL_SPARK_HADOOP}/bin/." "${SPARK_HOME}/bin/" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${SPECIAL_SPARK_HADOOP}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
        rm "spark-${SPARK_VERSION}-bin-hadoop${SPECIAL_SPARK_HADOOP}.tgz"

RUN export SPARK_HOME
ENV PATH $PATH:/usr/local/spark/bin
ENV PATH $PATH:/root/.local/bin

RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793

RUN apt-get update -qq && apt-get install vim -qqq

COPY requirements.txt .
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p ~/.postgresql && \
    wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O ~/.postgresql/root.crt && \
    chmod 0600 ~/.postgresql/root.crt

WORKDIR $AIRFLOW_HOME

COPY dags dags
COPY core core
COPY core /usr/local/spark/core

COPY csv csv
COPY csv /usr/local/spark/csv

COPY resources resources
COPY resources /usr/local/spark/resources

COPY data_generate_scripts data_generate_scripts
COPY data_generate_scripts /usr/local/spark/data_generate_scripts

# COPY scripts scripts
# RUN chmod +x scripts
