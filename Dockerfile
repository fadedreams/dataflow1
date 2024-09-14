# Use your custom base image
FROM base1-image:latest

# Set working directory (if needed)
WORKDIR /taskscripts

# Copy requirements file
# COPY requirements.txt ./

# Install Python dependencies from requirements.txt
# RUN pip install -r requirements.txt --progress-bar on

# Add Flink SQL connector Kafka jar
# ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar /jars

# Copy listener scripts
COPY listeners/* ./

# Define entrypoint or command as needed
# CMD ["python", "your_script.py"]
