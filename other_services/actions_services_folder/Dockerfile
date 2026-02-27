FROM python:3.11-slim

WORKDIR /action_service_app

COPY requirements.txt .

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python","actions_service.py"]
