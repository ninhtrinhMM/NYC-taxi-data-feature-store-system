FROM apache/airflow:2.8.1

USER root

# Cài Java 17 và dependencies
RUN apt-get update && \
    apt-get install -y \
        git \
        libgomp1 \
        openjdk-17-jdk-headless \
        procps \
        vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*



# Set environment variables cho Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python

USER airflow

# Cài đặt Delta Lake JAR files
RUN pip install --no-cache-dir delta-spark==2.4.0

# Cài requirements.txt trước
COPY --chown=airflow:root requirement.txt /tmp/requirement.txt
RUN pip install --no-cache-dir -r /tmp/requirement.txt

# Cài Airflow providers
RUN pip install --no-cache-dir \
    git+https://github.com/mpgreg/airflow-provider-great-expectations.git@87a42e275705d413cd4482134fc0d94fa1a68e6f \
    apache-airflow-providers-docker==3.7.5