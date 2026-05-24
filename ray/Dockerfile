FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY serve_app.py ./serve_app.py

# Ray head + serve en modo single-node
CMD ["python", "serve_app.py"]
