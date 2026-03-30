FROM python:3.14-slim

COPY summary/requirements.txt .

RUN apt-get update && apt-get install -y libpq-dev gcc && \
    pip3 install -r requirements.txt

COPY summary/dashboard.py .
COPY summary/queries.py .

# listen on port 8501 for Streamlit
EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]