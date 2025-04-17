# BigTable Transforms

The BigTable transforms allow you to write data to Google Cloud BigTable in your pipelines.

## WriteToBigTable

The `WriteToBigTable` transform writes data to Google Cloud BigTable.

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `project_id` | string | Yes | The Google Cloud project ID |
| `instance_id` | string | Yes | The BigTable instance ID |
| `table_id` | string | Yes | The BigTable table ID |
| `column_family_id` | string | Yes | The column family ID |
| `row_key_field` | string | Yes | The field to use as the row key |
| `batch_size` | integer | No | The number of rows to batch before writing (default: 100) |
| `max_retries` | integer | No | The maximum number of retries for failed writes (default: 3) |
| `skip_invalid_records` | boolean | No | Whether to skip invalid records (default: false) |

### Example

```yaml
- name: WriteToBigTable
  type: WriteToBigTable
  project_id: my-project
  instance_id: my-instance
  table_id: my-table
  column_family_id: my-family
  row_key_field: id
  batch_size: 100
  max_retries: 3
  skip_invalid_records: true
  inputs:
    - processed_data
  outputs:
    - bigtable_output
```

## Implementation Details

The `WriteToBigTable` transform uses the Google Cloud BigTable Python client library to write data to BigTable. It converts each input dictionary into a BigTable row, where:

1. The row key is derived from the specified `row_key_field` in the input dictionary
2. Each key-value pair in the input dictionary becomes a column in the specified column family
3. Nested dictionaries are flattened with keys joined by dots (e.g., `{"a": {"b": "c"}}` becomes `{"a.b": "c"}`)
4. Lists are converted to JSON strings

The transform supports batching writes for better performance and includes retry logic for handling transient errors.

## Local Testing with the BigTable Emulator

For local testing, you can use the Google Cloud BigTable emulator. To use the emulator:

1. Start the emulator using Docker:
   ```bash
   docker run -p 8086:8086 gcr.io/google.com/cloudsdktool/cloud-sdk:latest gcloud beta emulators bigtable start --host-port=0.0.0.0:8086
   ```

2. Set the `BIGTABLE_EMULATOR_HOST` environment variable:
   ```bash
   export BIGTABLE_EMULATOR_HOST=localhost:8086
   ```

3. Run your pipeline as usual. The `WriteToBigTable` transform will automatically connect to the emulator instead of the real BigTable service.

## Error Handling

The transform includes error handling for common issues:

1. **Invalid row keys**: If a record doesn't have the specified `row_key_field`, it will be skipped if `skip_invalid_records` is `true`, otherwise an error will be raised.
2. **Transient errors**: The transform will retry writes up to `max_retries` times with exponential backoff.
3. **Permanent errors**: If a write fails after all retries, the error will be logged and the pipeline will continue if `skip_invalid_records` is `true`, otherwise an error will be raised.

## Performance Considerations

For optimal performance:

1. Use an appropriate `batch_size` (100-1000 is usually a good range)
2. Consider using a distributed runner like Dataflow for large datasets
3. Ensure your row keys are well-distributed to avoid hotspotting
4. Use appropriate column families for your data model
