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
    redis-server \
    && pip install --upgrade pip \
    && pip install apache-airflow==${AIRFLOW_VERSION} --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install Redis, GraphQL, and other Python dependencies
RUN pip install redis graphene Flask Flask-GraphQL

# Copy project files
COPY . /app

# Start Redis server as a service (make sure Redis is running)
RUN service redis-server start

# Expose port for Airflow webserver
EXPOSE 8080

# Expose port for Redis (optional, if Redis needs external access)
EXPOSE 6379

# Start the Airflow scheduler, webserver, and Redis server
CMD bash -c "airflow db init && airflow scheduler & airflow webserver & service redis-server start"
