FROM python:3.11-slim

WORKDIR /app

# Install uv for dependency management
RUN pip install uv

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    netcat-traditional \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy pyproject.toml for dependencies
COPY pyproject.toml /app/

# Install dependencies using uv
RUN uv pip install -e . && \
    uv pip install jupyter jupyterlab google-cloud-bigtable

# Set up Jupyter
RUN mkdir -p /root/.jupyter && \
    echo "c.NotebookApp.token = ''" > /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.password = ''" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.ip = '0.0.0.0'" >> /root/.jupyter/jupyter_notebook_config.py

# Expose Jupyter port
EXPOSE 8888

# Start Jupyter Lab
CMD ["jupyter", "lab", "--allow-root", "--no-browser"]
