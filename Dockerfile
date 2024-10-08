# Use the official Bitnami Spark image as the base image
FROM bitnami/spark:latest

# Set environment variables
ENV SPARK_MODE=master
ENV SPARK_RPC_AUTHENTICATION_ENABLED=no
ENV SPARK_RPC_ENCRYPTION_ENABLED=no
ENV SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
ENV SPARK_SSL_ENABLED=no

# Set working directory
WORKDIR /opt/bitnami/spark

COPY requirements.txt .
RUN pip3 install -r requirements.txt

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
