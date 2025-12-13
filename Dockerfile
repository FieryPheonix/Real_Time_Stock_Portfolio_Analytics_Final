FROM apache/airflow:2.10.3

USER root

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME (Corrected for ARM64/Mac)
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64
ENV PATH "${JAVA_HOME}/bin:${PATH}"

# Verify Java installation
RUN java -version

# Copy requirements and set permissions
COPY requirements.txt /requirements.txt
RUN chown airflow: /requirements.txt

USER airflow

# Install Python packages
RUN pip install --no-cache-dir -r /requirements.txt