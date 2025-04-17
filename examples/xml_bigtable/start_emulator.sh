#!/bin/bash

# Start the BigTable emulator using Docker
echo "Starting BigTable emulator..."
cd ../../docker && docker-compose -f docker-compose-bigtable.yml up -d

# Wait for the emulator to start
echo "Waiting for the emulator to start..."
sleep 5

# Set the environment variable for the BigTable emulator
export BIGTABLE_EMULATOR_HOST=localhost:8086
echo "BIGTABLE_EMULATOR_HOST set to localhost:8086"

echo "BigTable emulator is now running."
echo "You can run the pipeline with: python run_pipeline.py"
echo "To stop the emulator, run: ./stop_emulator.sh"
