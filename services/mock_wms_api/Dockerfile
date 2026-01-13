FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn[standard] "pydantic>=2,<3"

COPY app /app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
