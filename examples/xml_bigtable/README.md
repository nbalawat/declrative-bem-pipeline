# XML to BigTable Pipeline Example

This example demonstrates how to use the declarative beam pipeline framework to:
1. Read and parse XML data
2. Transform the data
3. Write the results to Google Cloud BigTable (using the BigTable emulator for local testing)

## Prerequisites

- Docker and Docker Compose installed
- Python 3.8+ with uv installed

## Setup

1. Install the required dependencies:

```bash
cd /Users/nbalawat/development/declarative-beam-pipeline
uv sync
```

2. Start the BigTable emulator:

```bash
cd examples/xml_bigtable
chmod +x start_emulator.sh
./start_emulator.sh
```

This will:
- Start the BigTable emulator in a Docker container
- Set the `BIGTABLE_EMULATOR_HOST` environment variable to `localhost:8086`

## Running the Example

Once the emulator is running, you can run the example pipeline:

```bash
python run_pipeline.py
```

This will:
1. Read the XML data from `sample_data.xml`
2. Parse the XML into dictionaries
3. Enrich the data with additional fields
4. Filter for expensive products (price > $100)
5. Write the results to the BigTable emulator
6. Print the contents of the BigTable table

## Stopping the Emulator

When you're done, stop the BigTable emulator:

```bash
chmod +x stop_emulator.sh
./stop_emulator.sh
```

## Pipeline Configuration

The pipeline is defined in `pipeline.yaml` and includes the following transforms:

- `ReadFromXml`: Reads and parses XML data
- `EnrichProducts`: Adds additional fields to the product data
- `FilterExpensiveProducts`: Filters products with a price > $100
- `WriteToBigTable`: Writes the filtered products to BigTable
- `WriteResults`: Writes the results to a text file for verification

## Understanding the Code

- `run_pipeline.py`: Main script to run the pipeline
- `utils.py`: Utility functions for transforming the data
- `setup_bigtable.py`: Script to set up the BigTable emulator
- `pipeline.yaml`: Pipeline configuration
- `sample_data.xml`: Sample XML data

## Extending the Example

You can extend this example by:
1. Adding more transformations to the pipeline
2. Modifying the XML parsing logic
3. Adding more complex BigTable operations
4. Implementing error handling and dead-letter queues

## Notes on Production Use

For production use:
1. Replace the emulator with a real BigTable instance
2. Add authentication using service accounts
3. Implement proper error handling and monitoring
4. Consider using a streaming source for real-time processing
