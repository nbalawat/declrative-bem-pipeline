# I/O Transforms

This section documents the I/O transforms available in the Declarative Beam Pipeline framework for reading from and writing to various data sources.

## Reading Data

- [ReadFromText](read_from_text.md): Read data from text files
- [ReadFromCsv](read_from_csv.md): Read data from CSV files
- [ReadFromJson](read_from_json.md): Read data from JSON files
- [ReadFromAvro](read_from_avro.md): Read data from Avro files
- [ReadFromParquet](read_from_parquet.md): Read data from Parquet files

## Writing Data

- [WriteToText](write_to_text.md): Write data to text files
- [WriteToCsv](write_to_csv.md): Write data to CSV files
- [WriteToJson](write_to_json.md): Write data to JSON files
- [WriteToAvro](write_to_avro.md): Write data to Avro files
- [WriteToParquet](write_to_parquet.md): Write data to Parquet files

## Example: Reading from Text Files

```yaml
- name: ReadCSV
  type: ReadFromText
  config:
    file_pattern: input.csv
  outputs:
    - raw_lines
```

## Example: Writing to Text Files

```yaml
- name: WriteResults
  type: WriteToText
  config:
    file_path_prefix: output/results
    compression_type: gzip  # Optional
  inputs:
    - formatted_results
```

## Supported Compression Types

The following compression types are supported for both reading and writing:

- `auto`: Automatically detect the compression type (reading only)
- `gzip`: GZIP compression
- `bzip2`: BZIP2 compression
- `deflate`: DEFLATE compression
- `uncompressed`: No compression

## File Patterns

For reading data, you can use glob patterns to match multiple files:

- `*.csv`: Match all CSV files in the current directory
- `data/*.csv`: Match all CSV files in the data directory
- `data/input-*.csv`: Match files like data/input-1.csv, data/input-2.csv, etc.
- `data/input-???.csv`: Match files with exactly three characters after "input-"

## Handling Large Datasets

When working with large datasets, consider the following:

- Use compression to reduce storage and network costs
- Split large files into smaller chunks for parallel processing
- Use appropriate windowing strategies for streaming data

For more advanced I/O operations, such as reading from databases or cloud storage, see the [Advanced I/O](../../advanced/advanced_io.md) section.
