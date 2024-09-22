# Base image: Use the official Python image
FROM python:3.9-slim

# Set environment variables for Airflow
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_VERSION=2.3.3
ENV PYTHON_VERSION=3.9

# Set working directory
WORKDIR /app

# Install system dependencies and Airflow
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    libssl-dev \
    libffi-dev \
    python3-dev \
    && pip install --upgrade pip \
    && pip install apache-airflow==${AIRFLOW_VERSION} --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Copy project files
COPY . /app


# Expose port for Airflow webserver
EXPOSE 8080

# Start the Airflow scheduler and webserver
CMD bash -c "airflow db init && airflow scheduler & airflow webserver"
