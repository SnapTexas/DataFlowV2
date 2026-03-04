# Use a stable, lightweight Python version
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies (optional but good for networking tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file first to leverage Docker caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY . .

# Run the script (replace 'bridge.py' with your actual filename)
CMD ["python","-u", "lead_bridge_service.py"]