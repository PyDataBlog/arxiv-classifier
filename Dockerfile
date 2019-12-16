# Pull the base image
FROM tensorflow/tensorflow:2.0.0-py3

# Maintainer Info
LABEL maintainer="bbrenyah@gmail.com"

# Create working folder
RUN mkdir -p /home/arxiv-classifier && \
    apt update --fix-missing && \
    apt-get install -y firefox sqlite3

# Set the newly created folder as the working directory
WORKDIR /home/arxiv-classifier

# Upgrade pip with no cache
RUN pip install --no-cache-dir -U pip

# Copy the requirements file to working directory
COPY requirements.txt .

# Install the required applications
RUN pip install -r requirements.txt

# Copy everything in the source folder to the working directory
COPY . .

# Run the scraper
RUN ["python", "app/scraper.py"]