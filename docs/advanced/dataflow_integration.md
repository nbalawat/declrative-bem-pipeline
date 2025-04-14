# Dataflow Integration

This guide explains how to run declarative beam pipelines on Google Cloud Dataflow for production workloads.

## Prerequisites

Before running pipelines on Dataflow, ensure you have:

1. A Google Cloud Platform (GCP) account with Dataflow API enabled
2. The `apache-beam[gcp]` package installed (included in the project dependencies)
3. Proper authentication set up for GCP
4. A GCS bucket for temporary files and pipeline outputs

## Setting Up Authentication

To authenticate with GCP, you can use one of the following methods:

```bash
# Option 1: Use gcloud CLI
gcloud auth application-default login

# Option 2: Use a service account key file
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

## Running a Pipeline on Dataflow

To run a pipeline on Dataflow, you need to:

1. Configure the pipeline options for Dataflow
2. Build the pipeline using the YAML configuration
3. Run the pipeline with the Dataflow runner

Here's an example script:

```python
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions

from declarative_beam.core.yaml_processor import YAMLProcessor
from declarative_beam.core.pipeline_builder import PipelineBuilder

def run_pipeline_on_dataflow(yaml_file, project, region, temp_location, staging_location):
    # Set up logging
    logging.getLogger().setLevel(logging.INFO)
    
    # Load the YAML configuration
    yaml_processor = YAMLProcessor()
    pipeline_config = yaml_processor.load_yaml(yaml_file)
    
    # Configure pipeline options
    options = PipelineOptions()
    
    # Set Google Cloud options
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project
    google_cloud_options.region = region
    google_cloud_options.job_name = f"declarative-beam-{pipeline_config.get('name', 'pipeline').lower().replace(' ', '-')}"
    google_cloud_options.temp_location = temp_location
    google_cloud_options.staging_location = staging_location
    
    # Set standard options
    standard_options = options.view_as(StandardOptions)
    standard_options.runner = 'DataflowRunner'
    
    # Set up options
    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True  # Save the main session state
    
    # Build and run the pipeline
    builder = PipelineBuilder(pipeline_config, options=options)
    pipeline = builder.build_pipeline()
    result = pipeline.run()
    
    # Wait for the pipeline to finish
    result.wait_until_finish()
    
    return result

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a declarative beam pipeline on Dataflow')
    parser.add_argument('--yaml_file', required=True, help='Path to the YAML configuration file')
    parser.add_argument('--project', required=True, help='GCP project ID')
    parser.add_argument('--region', required=True, help='GCP region (e.g., us-central1)')
    parser.add_argument('--temp_location', required=True, help='GCS path for temporary files')
    parser.add_argument('--staging_location', required=True, help='GCS path for staging files')
    
    args = parser.parse_args()
    
    run_pipeline_on_dataflow(
        args.yaml_file,
        args.project,
        args.region,
        args.temp_location,
        args.staging_location
    )
```

## File Paths in Dataflow

When running on Dataflow, file paths should use GCS paths instead of local paths:

```yaml
# Local path (for development)
file_pattern: data/input.csv

# GCS path (for Dataflow)
file_pattern: gs://your-bucket/data/input.csv
```

## Adapting YAML Configuration for Dataflow

You may need to adapt your YAML configuration for Dataflow:

```yaml
name: Example Pipeline for Dataflow
description: A pipeline that runs on Dataflow

transforms:
  - name: ReadCSV
    type: ReadFromText
    config:
      file_pattern: gs://your-bucket/data/input.csv
    outputs:
      - raw_lines

  # ... other transforms ...

  - name: WriteResults
    type: WriteToText
    config:
      file_path_prefix: gs://your-bucket/output/results
    inputs:
      - formatted_results
```

## Handling Dependencies

If your pipeline uses custom modules or packages, you need to ensure they're available on Dataflow workers:

1. **Option 1**: Package your code as a Python package and specify it in `setup_options.setup_file`
2. **Option 2**: Use `setup_options.requirements_file` to specify dependencies
3. **Option 3**: Use `setup_options.extra_packages` to include additional packages

Example:

```python
setup_options = options.view_as(SetupOptions)
setup_options.setup_file = './setup.py'  # Your package's setup file
setup_options.requirements_file = './requirements.txt'  # Dependencies
setup_options.extra_packages = ['./dist/my_custom_package-1.0.0.tar.gz']  # Additional packages
```

## Monitoring Dataflow Jobs

You can monitor your Dataflow jobs in the Google Cloud Console:

1. Go to the Dataflow page in the GCP Console
2. Find your job in the list (it will have the job name you specified)
3. Click on the job to view details, metrics, and logs

## Streaming Pipelines

For streaming pipelines, set the processing mode to streaming:

```python
standard_options = options.view_as(StandardOptions)
standard_options.streaming = True
```

Your YAML configuration should use appropriate windowing transforms for streaming data.

## Cost Optimization

To optimize costs when running on Dataflow:

1. **Worker Type**: Choose appropriate machine types for your workload
2. **Autoscaling**: Configure autoscaling to match your workload
3. **Disk Size**: Set an appropriate disk size for workers
4. **Region**: Choose a region with lower costs
5. **Batch vs. Streaming**: Use batch processing when possible

Example configuration:

```python
from apache_beam.options.pipeline_options import WorkerOptions

worker_options = options.view_as(WorkerOptions)
worker_options.machine_type = 'n1-standard-2'
worker_options.max_num_workers = 10
worker_options.disk_size_gb = 30
```

## Troubleshooting

Common issues and solutions:

1. **Authentication errors**: Check your authentication setup
2. **Missing dependencies**: Ensure all dependencies are properly packaged
3. **File not found errors**: Verify GCS paths and permissions
4. **Quota exceeded**: Request quota increases if needed
5. **Performance issues**: Check for bottlenecks in your pipeline

## Example: Complete Dataflow Pipeline

Here's a complete example of running a data processing pipeline on Dataflow:

```python
import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions

from declarative_beam.core.yaml_processor import YAMLProcessor
from declarative_beam.core.pipeline_builder import PipelineBuilder

def run_dataflow_pipeline(args):
    # Set up logging
    logging.getLogger().setLevel(logging.INFO)
    
    # Load the YAML configuration
    yaml_processor = YAMLProcessor()
    pipeline_config = yaml_processor.load_yaml(args.yaml_file)
    
    # Configure pipeline options
    options = PipelineOptions()
    
    # Set Google Cloud options
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = args.project
    google_cloud_options.region = args.region
    google_cloud_options.job_name = f"declarative-beam-{pipeline_config.get('name', 'pipeline').lower().replace(' ', '-')}"
    google_cloud_options.temp_location = args.temp_location
    google_cloud_options.staging_location = args.staging_location
    
    # Set standard options
    standard_options = options.view_as(StandardOptions)
    standard_options.runner = 'DataflowRunner'
    
    # Set streaming mode if specified
    if args.streaming:
        standard_options.streaming = True
    
    # Set up options
    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True
    
    # Set worker options
    if args.machine_type or args.max_workers or args.disk_size_gb:
        from apache_beam.options.pipeline_options import WorkerOptions
        worker_options = options.view_as(WorkerOptions)
        
        if args.machine_type:
            worker_options.machine_type = args.machine_type
        
        if args.max_workers:
            worker_options.max_num_workers = args.max_workers
        
        if args.disk_size_gb:
            worker_options.disk_size_gb = args.disk_size_gb
    
    # Build and run the pipeline
    builder = PipelineBuilder(pipeline_config, options=options)
    pipeline = builder.build_pipeline()
    result = pipeline.run()
    
    # Wait for the pipeline to finish if not streaming
    if not args.streaming:
        result.wait_until_finish()
    
    return result

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a declarative beam pipeline on Dataflow')
    parser.add_argument('--yaml_file', required=True, help='Path to the YAML configuration file')
    parser.add_argument('--project', required=True, help='GCP project ID')
    parser.add_argument('--region', required=True, help='GCP region (e.g., us-central1)')
    parser.add_argument('--temp_location', required=True, help='GCS path for temporary files')
    parser.add_argument('--staging_location', required=True, help='GCS path for staging files')
    parser.add_argument('--streaming', action='store_true', help='Run in streaming mode')
    parser.add_argument('--machine_type', help='Worker machine type (e.g., n1-standard-2)')
    parser.add_argument('--max_workers', type=int, help='Maximum number of workers')
    parser.add_argument('--disk_size_gb', type=int, help='Worker disk size in GB')
    
    args = parser.parse_args()
    
    run_dataflow_pipeline(args)
```

## Conclusion

Running declarative beam pipelines on Dataflow allows you to process large datasets efficiently and reliably. By following the guidelines in this document, you can successfully deploy your pipelines to Dataflow for production workloads.
