FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .

RUN pip3 install -r requirements.txt 

COPY dashboard.py .

COPY live/ ./live/
COPY historical/ ./historical/
COPY summary/ ./summary/
COPY trends/ ./trends/

# listen on port 8501 for Streamlit
EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]