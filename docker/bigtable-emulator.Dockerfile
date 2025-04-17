FROM google/cloud-sdk:alpine

# Install required packages
RUN apk add --no-cache openjdk11-jre python3 py3-pip

# Install the beta components which include bigtable emulator
RUN gcloud components install beta bigtable cbt --quiet

# Set environment variables
ENV BIGTABLE_EMULATOR_HOST=0.0.0.0:8086

# Expose the emulator port
EXPOSE 8086

# Start the bigtable emulator
CMD ["gcloud", "beta", "emulators", "bigtable", "start", "--host-port=0.0.0.0:8086"]
