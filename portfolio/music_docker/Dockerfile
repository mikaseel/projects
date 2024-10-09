# Use Ubuntu 22.04 LTS as base
FROM ubuntu:22.04

# Set environment variables
ENV AIRFLOW_HOME=/usr/local/airflow
ENV JUPYTER_CONFIG_DIR=/home/lexi/.jupyter

# Install necessary system packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    postgresql-client \
    python3-pip \
    vim \
    nano \
    sudo && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Add user 'lexi'
RUN useradd -ms /bin/bash lexi&& \
    echo "lexi:lexi123" | chpasswd && \
    usermod -aG sudo lexi # Add lexi user to sudo group

# Install JupyterLab and Airflow
RUN pip install jupyterlab==4.1.5 apache-airflow==2.8.4

# Expose the ports for JupyterLab and Airflow
EXPOSE 8888 8080

# Set up directories for Airflow and JupyterLab
RUN mkdir -p $AIRFLOW_HOME $AIRFLOW_HOME/dags $JUPYTER_CONFIG_DIR && \
    chown -R lexi:lexi $AIRFLOW_HOME $JUPYTER_CONFIG_DIR

# Set the working directory
WORKDIR /home/lexi

# Copy the requirements.txt file into the container at /home/lexi
COPY requirements.txt .

# Install Python packages from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt