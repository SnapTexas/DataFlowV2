FROM python:3.12-slim

WORKDIR /app

# Install the correct library for Python 3.12
RUN pip install kafka-python-ng

COPY producer.py .

CMD ["python", "producer.py"]