FROM python:3.7

RUN mkdir /app
WORKDIR /app
COPY main.py .
COPY requirements.txt .
RUN pip install -r requirements.txt

EXPOSE 5000
CMD ["python", "/app/main.py"]
