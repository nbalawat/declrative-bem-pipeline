# Installation Guide

This guide walks you through the process of installing the Declarative Beam Pipeline framework.

## Prerequisites

- Python 3.8 or later
- `uv` package manager (recommended)

## Installation Steps

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/declarative-beam-pipeline.git
cd declarative-beam-pipeline
```

### 2. Create a Virtual Environment and Install Dependencies

We recommend using `uv` for dependency management:

```bash
# Create a virtual environment
python -m venv .venv

# Activate the virtual environment
# On macOS/Linux:
source .venv/bin/activate
# On Windows:
# .venv\Scripts\activate

# Install dependencies using uv
uv sync
```

Alternatively, you can use pip:

```bash
pip install -e .
```

### 3. Verify Installation

Run a simple test to verify that the installation was successful:

```bash
python tests/test_pipeline_runner.py
```

You should see output indicating that the tests have passed.

## Installing for Development

If you plan to contribute to the framework, install the development dependencies:

```bash
uv sync -e dev
```

## Docker Installation

For containerized deployment, we provide Docker support:

```bash
# Build the Docker image
docker build -t declarative-beam-pipeline -f docker/Dockerfile .

# Run the container
docker run -it declarative-beam-pipeline
```

The Docker image includes Jupyter Lab for interactive development.

## Installing GCP Components

If you plan to run pipelines on Google Cloud Dataflow, make sure you have the GCP components installed:

```bash
# The framework already includes apache-beam[gcp] as a dependency
# But you may need to authenticate with GCP
gcloud auth application-default login
```

## Troubleshooting

If you encounter any issues during installation:

1. Make sure you're using a compatible Python version (3.8+)
2. Check that all dependencies are properly installed
3. Verify that your Apache Beam version is compatible (2.40.0+)
4. For GCP-related issues, ensure you have proper authentication set up

For more help, please open an issue on the GitHub repository.
