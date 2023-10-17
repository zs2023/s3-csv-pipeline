# Use Ubuntu 22.04 as a parent image
FROM ubuntu:22.04

# Set the maintainer label
LABEL maintainer="zhongyi.sun.17@gmail.com"

# Run package updates and install packages
RUN apt-get update \
    && apt-get install -y python3-pip python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Java for spark
RUN apt-get update \
    && apt-get install -y openjdk-11-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies 
RUN pip3 install pyspark
RUN pip3 install boto3
RUN pip3 install pyyaml

# Copy python and yaml files into the working directory
COPY ./main.py /main.py
COPY ./config.yaml /config.yaml

# Make port 80 available to the internet
EXPOSE 80

# Run bash when the container launches
CMD ["/bin/bash"]