# Stage 1: Build Google Cloud SDK with Python 3.9
FROM python:3.9-slim as builder

WORKDIR /app

# Install necessary packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    python3 \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the application files
COPY . /app

# Download and install Google Cloud SDK
RUN curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/google-cloud-sdk.tar.gz && \
    tar -zxvf google-cloud-sdk.tar.gz && \
    CLOUDSDK_PYTHON=python3 ./google-cloud-sdk/install.sh

# Authenticate with Google Cloud
RUN ./google-cloud-sdk/bin/gcloud auth activate-service-account --key-file=/app/cloud-storage.json

# Download the 'location.shp' file from Google Cloud Storage to /app
RUN ./google-cloud-sdk/bin/gsutil cp gs://gis_regions/location.shp /app/location.shp

# Stage 2: Final image with Python 3.7
FROM python:3.7-slim

WORKDIR /app

# Copy the application files from the builder stage
COPY --from=builder /app /app

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Install python-multipart
RUN pip install python-multipart

# Make port 80 available to the world outside this container
EXPOSE 8008

# Run app.py when the container launches
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8008"]
