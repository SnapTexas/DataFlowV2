# Use a stable, lightweight Python version
FROM python:3.12-slim

# Set the working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all code (bridge.py, validation.py, etc.)
COPY . .

# Default command (can be overridden by docker-compose)
CMD ["python", "-u", "lead_data_validation_service.py"]