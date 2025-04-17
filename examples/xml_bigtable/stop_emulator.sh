#!/bin/bash

# Stop the BigTable emulator using Docker
echo "Stopping BigTable emulator..."
cd ../../docker && docker-compose -f docker-compose-bigtable.yml down

echo "BigTable emulator has been stopped."
